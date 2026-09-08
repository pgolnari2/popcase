from unittest.mock import patch

from django.test import SimpleTestCase

from popcase.incidence_rates import (
    _population_exposures, subcounty_incidence, load_decennial_population, validate_tract_rate_period,
)
from popcase.rate_statistics import (
    RateDataUnavailable, age_bands, crude_rate, indirect_rate, indirect_rate_ci, byar_count_limits,
    population_year_exposure, query_year_exposure, selected_age_bands,
)


class IncidenceMethodTests(SimpleTestCase):
    def test_literal_controller_sir_times_target_crude(self):
        self.assertAlmostEqual(indirect_rate(38, {'young': 9000, 'old': 1000},
                                            {'young': 10, 'old': 100},
                                            {'young': 10000, 'old': 10000}), 760)

    def test_exact_poisson_crude_limits(self):
        rate, lower, upper = crude_rate(10, 100000)
        self.assertEqual(rate, 10)
        self.assertAlmostEqual(lower, 4.7953887, places=6)
        self.assertAlmostEqual(upper, 18.3903560, places=6)
        self.assertAlmostEqual(crude_rate(0, 100000)[2], 3.6888795, places=6)

    def test_required_age_groups_and_periods(self):
        self.assertEqual(len(age_bands('tract')), 20)
        for level in ('zcta', 'place'):
            self.assertEqual(len(age_bands(level)), 18)
        self.assertEqual(selected_age_bands('tract', {'age_groups': ['age_5_9']}), ((5, 9),))
        with self.assertRaises(RateDataUnavailable):
            selected_age_bands('zcta', {'age_from': 1, 'age_to': 4})
        years = query_year_exposure({'dx_start': '2013', 'dx_end': '2022'}, 2022)
        self.assertEqual(population_year_exposure(years, True), {2010: 3, 2020: 7})
        self.assertEqual(query_year_exposure({'dx_start': '2021Q4', 'dx_end': '2022Q1'}, 2022),
                         {2021: .25, 2022: .25})

    def test_missing_population_is_not_a_zero(self):
        valid, errors = _population_exposures([], {2020: 1}, ((0, 4),), ('1',), ('1',),
                                             {('39001', 2019)})
        self.assertEqual(valid, {})
        self.assertIn('39001', errors)
        with self.assertRaises(RateDataUnavailable):
            indirect_rate(10, {'a': 1000}, {'a': 10}, {})

    def test_each_subcounty_rate_and_byar_ci(self):
        for level, geoid in (('tract', '39001000100'), ('zcta', '43001'), ('place', '3900100')):
            band = age_bands(level)[0]
            with self.subTest(level=level), \
                 patch('popcase.incidence_rates.validate_tract_rate_period'), \
                 patch('popcase.incidence_rates.load_case_counts',
                       return_value=({geoid: 20}, {geoid: {band: 20}}, {band: 10}, {})), \
                 patch('popcase.incidence_rates.load_target_populations',
                       return_value=({geoid: {band: 10000}}, {})), \
                 patch('popcase.incidence_rates.load_ohio_annual_population', return_value={band: 10000}), \
                 patch('popcase.incidence_rates.load_ohio_decennial_population', return_value={band: 10000}):
                result = subcounty_incidence('2022', level, {'dx_start': '2022', 'dx_end': '2022'})[0]
                self.assertEqual(result['crude_incidence_per_100k'], 200)
                self.assertEqual(result['age_adjusted_per_100k'], 400)
                self.assertIsNotNone(result['crude_incidence_ci_lower'])
                self.assertLess(result['age_adjusted_ci_lower'], 400)
                self.assertGreater(result['age_adjusted_ci_upper'], 400)

    def test_byar_limits(self):
        # Published PHE Byar example: 12 events / 1537 population.
        lower, upper = byar_count_limits(12)
        self.assertAlmostEqual(lower * 100000 / 1537, 402.961, places=2)
        self.assertAlmostEqual(upper * 100000 / 1537, 1363.881, places=2)
        rate, lower, upper = indirect_rate_ci(0, {'a': 1000}, {'a': 10}, {'a': 1000})
        self.assertEqual((rate, lower), (0, 0))
        self.assertGreater(upper, 0)

    def test_original_census_sex_cells_across_both_censuses(self):
        with patch('popcase.incidence_rates.connections') as db:
            cursor = db.__getitem__.return_value.cursor.return_value.__enter__.return_value
            cursor.fetchall.side_effect = [[('3900001', 10, 20)], [('3900001', 30, 40)]]
            populations, errors = load_decennial_population(
                {2015: 1, 2016: 1}, ((0, 4),), None, 'place')
        self.assertEqual(errors, {})
        self.assertEqual(populations['3900001'][(0, 4)], 100)

    def test_unsupported_tract_period_rejected_before_loading_populations(self):
        with patch('popcase.incidence_rates.connections') as db:
            cursor = db.__getitem__.return_value.cursor.return_value.__enter__.return_value
            cursor.fetchall.return_value = [('2010',), ('2023',)]
            with self.assertRaisesRegex(RateDataUnavailable, 'please change the diagnosis period'):
                validate_tract_rate_period({'dx_start': '1950q3', 'dx_end': '2024q4'}, 2023)
            validate_tract_rate_period({'dx_start': '2023q1', 'dx_end': '2023q4'}, 2023)
