"""Age groups, population periods, crude CIs and SIR adjustment from Controller Logic."""

import math
import re
from collections import defaultdict

from scipy.stats import chi2


class RateDataUnavailable(ValueError):
    """The requested rate cannot be calculated with matched, complete inputs."""


AGE_BANDS_20 = ((0, 0), (1, 4)) + tuple((n, n + 4) for n in range(5, 90, 5)) + ((90, None),)
AGE_BANDS_18 = ((0, 4),) + tuple((n, n + 4) for n in range(5, 85, 5)) + ((85, None),)


def age_bands(geographic_level):
    return AGE_BANDS_18 if geographic_level in {"zcta", "place"} else AGE_BANDS_20


def age_band_for_age(age, geographic_level):
    try:
        value = int(age)
    except (TypeError, ValueError):
        return None
    # NAACCR 999 is unknown, not an age in the oldest population stratum.
    if value < 0 or value >= 999:
        return None
    return next((band for band in age_bands(geographic_level)
                 if value >= band[0] and (band[1] is None or value <= band[1])), None)


def selected_age_bands(geographic_level, filters):
    bands = age_bands(geographic_level)
    tokens = filters.get("age_groups") or []
    if isinstance(tokens, str):
        tokens = [tokens]
    ranges = []
    for token in tokens:
        match = re.fullmatch(r"age_(\d+)(?:_(\d+|plus))?", token)
        if not match:
            raise RateDataUnavailable("Unrecognized age selection.")
        low, high = match.groups()
        ranges.append((int(low), None if high == "plus" else int(high or low)))
    if not ranges and any(filters.get(key) not in (None, "") for key in ("age_from", "age_to")):
        ranges = [(int(filters.get("age_from") or 0),
                   int(filters["age_to"]) if filters.get("age_to") not in (None, "") else None)]
    if not ranges:
        return bands
    # A query may combine adjacent selections, but must not split a source band.
    selected = set()
    for low, high in ranges:
        if low < 0 or (high is not None and high < low):
            raise RateDataUnavailable("Invalid age range.")
        selected.update(range(low, (high if high is not None else 998) + 1))
    result = []
    for band in bands:
        ages = set(range(band[0], (band[1] if band[1] is not None else 998) + 1))
        if selected & ages:
            if not ages <= selected:
                raise RateDataUnavailable("The selected ages split an available population age group.")
            result.append(band)
    if not result:
        raise RateDataUnavailable("No supported population age groups were selected.")
    return tuple(result)


def query_year_exposure(filters, default_year):
    """Return years of exposure (including quarter fractions) per calendar year."""
    def boundary(value, end):
        match = re.fullmatch(r"(\d{4})(?:[qQ]([1-4]))?", str(value).strip())
        if not match:
            raise RateDataUnavailable("A valid diagnosis year or quarter range is required.")
        return int(match[1]), int(match[2]) if match[2] else (4 if end else 1)

    start = boundary(filters.get("dx_start") or default_year, False)
    end = boundary(filters.get("dx_end") or default_year, True)
    if start > end:
        raise RateDataUnavailable("The diagnosis period ends before it starts.")
    exposure = defaultdict(float)
    for quarter in range(start[0] * 4 + start[1] - 1, end[0] * 4 + end[1]):
        exposure[quarter // 4] += 0.25
    return dict(exposure)


def population_year_exposure(year_exposure, decennial=False):
    """Exposure times population equals person-years, not just a mean population."""
    result = defaultdict(float)
    for year, duration in year_exposure.items():
        result[(2010 if year < 2016 else 2020) if decennial else year] += duration
    return dict(result)


def poisson_count_limits(count, confidence=0.95):
    count = float(count)
    if not math.isfinite(count) or count < 0 or not count.is_integer():
        raise RateDataUnavailable("Exact Poisson intervals require a nonnegative integer case count.")
    if not 0 < confidence < 1:
        raise ValueError("Confidence must be between zero and one.")
    tail = (1 - confidence) / 2
    lower = 0.0 if count == 0 else float(chi2.ppf(tail, 2 * count) / 2)
    upper = float(chi2.isf(tail, 2 * (count + 1)) / 2)
    return lower, upper


def crude_rate(count, person_years, multiplier=100000.0):
    person_years = float(person_years)
    if not math.isfinite(person_years) or person_years <= 0:
        raise RateDataUnavailable("A positive population person-year denominator is required.")
    lower, upper = poisson_count_limits(count)
    scale = multiplier / person_years
    return float(count) * scale, lower * scale, upper * scale


def indirect_rate(observed, target_person_years, reference_cases, reference_person_years):
    """Controller Logic: observed/expected multiplied by the target crude rate."""
    expected = 0.0
    for band, exposure in target_person_years.items():
        if not math.isfinite(exposure) or exposure < 0:
            raise RateDataUnavailable("Invalid target population exposure.")
        if exposure == 0:
            continue
        reference_exposure = reference_person_years.get(band)
        if reference_exposure is None or not math.isfinite(reference_exposure) or reference_exposure <= 0:
            raise RateDataUnavailable("Ohio population data are missing for a selected age group.")
        count = reference_cases.get(band, 0)
        if count < 0 or not math.isfinite(count):
            raise RateDataUnavailable("Invalid Ohio case count.")
        expected += exposure * count / reference_exposure
    total_exposure = sum(target_person_years.values())
    if total_exposure <= 0:
        raise RateDataUnavailable("A positive population denominator is required.")
    if expected <= 0:
        raise RateDataUnavailable("The expected case count is zero; an SIR is undefined.")
    return (observed / expected) * (100000.0 * observed / total_exposure)


def byar_count_limits(observed):
    # https://fingertips.phe.org.uk/static-reports/public-health-technical-guidance/Basic_statistics/Rates.html
    lower = 0.0 if observed == 0 else observed * (1 - 1 / (9 * observed) - 1.959963984540054 / (3 * math.sqrt(observed))) ** 3
    upper = (observed + 1) * (1 - 1 / (9 * (observed + 1)) + 1.959963984540054 / (3 * math.sqrt(observed + 1))) ** 3
    return max(0.0, lower), upper


def indirect_rate_ci(observed, target_person_years, reference_cases, reference_person_years):
    lower, upper = byar_count_limits(observed)
    # Apply the same SIR × target-crude formula to the Byar count limits.
    # Both factors depend on the count; Ohio reference rates are held fixed.
    scale = indirect_rate(1, target_person_years, reference_cases, reference_person_years)
    return tuple(count * count * scale for count in (observed, lower, upper))
