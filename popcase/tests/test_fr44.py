from django.test import SimpleTestCase

from popcase.services import (
    _acs_estimate_ci,
    _acs_pct_ci_from_num_denom,
    _apply_display_option_contract,
)


class FR44ConfidenceIntervalTests(SimpleTestCase):
    def test_negative_acs_moe_sentinel_is_treated_as_zero(self):
        self.assertEqual(_acs_estimate_ci(1000, -555555555, ndigits=0), (1000.0, 1000.0))
        self.assertEqual(
            _acs_pct_ci_from_num_denom(600, -555555555, 1000, -555555555),
            (60.0, 60.0),
        )

    def test_basic_demographic_ci_is_removed_when_not_requested(self):
        row = {
            "total_population": 1000,
            "total_population_ci_lower": 950,
            "total_population_ci_upper": 1050,
            "female_pct": 51.2,
            "female_pct_ci_lower": 49.0,
            "female_pct_ci_upper": 53.4,
            "male_population": 488,
            "female_population": 512,
            "male_pct": 48.8,
            "male_pct_ci_lower": 46.6,
            "male_pct_ci_upper": 51.0,
            "sex_distribution": "Male/Female",
        }

        _apply_display_option_contract(row, ["pop_total", "sex_distribution"], [])

        self.assertEqual(
            row,
            {
                "total_population": 1000,
                "female_pct": 51.2,
                "male_population": 488,
                "female_population": 512,
                "male_pct": 48.8,
                "sex_distribution": "Male/Female",
            },
        )

    def test_basic_demographic_sex_fields_and_ci_are_kept_when_requested(self):
        row = {
            "total_population": 1000,
            "total_population_ci_lower": 950,
            "total_population_ci_upper": 1050,
            "female_pct": 51.2,
            "female_pct_ci_lower": 49.0,
            "female_pct_ci_upper": 53.4,
            "male_population": 488,
            "female_population": 512,
            "male_pct": 48.8,
            "male_pct_ci_lower": 46.6,
            "male_pct_ci_upper": 51.0,
        }

        _apply_display_option_contract(
            row,
            ["pop_total", "sex_distribution"],
            ["community_basic_ci"],
        )

        self.assertEqual(
            row,
            {
                "total_population": 1000,
                "total_population_ci_lower": 950,
                "total_population_ci_upper": 1050,
                "female_pct": 51.2,
                "female_pct_ci_lower": 49.0,
                "female_pct_ci_upper": 53.4,
                "male_population": 488,
                "female_population": 512,
                "male_pct": 48.8,
                "male_pct_ci_lower": 46.6,
                "male_pct_ci_upper": 51.0,
            },
        )

    def test_unrequested_ci_and_age_adjusted_fields_are_removed_with_period_variants(self):
        row = {
            "routine_checkup_pct": 72.0,
            "routine_checkup_ci_lower": 70.0,
            "routine_checkup_ci_upper": 74.0,
            "routine_checkup_age_adjusted_pct": 71.0,
            "routine_checkup_ci_lower__acs_2019_2023": 69.0,
        }

        _apply_display_option_contract(row, ["routine_checkup"], [])

        self.assertEqual(row, {"routine_checkup_pct": 72.0})

    def test_fr37_cancer_prevention_ci_follows_checkbox(self):
        unchecked = {
            "cervical_screening_pct": 81.0,
            "cervical_screening_ci_lower": 79.0,
            "cervical_screening_ci_upper": 83.0,
        }
        _apply_display_option_contract(unchecked, ["cervical_screen"], [])
        self.assertEqual(unchecked, {"cervical_screening_pct": 81.0})

        checked = {
            "cervical_screening_pct": 81.0,
            "cervical_screening_ci_lower": 79.0,
            "cervical_screening_ci_upper": 83.0,
        }
        _apply_display_option_contract(
            checked,
            ["cervical_screen"],
            ["cancer_screening_ci"],
        )
        self.assertIn("cervical_screening_ci_lower", checked)
        self.assertIn("cervical_screening_ci_upper", checked)

    def test_fr40_access_ci_follows_each_geography_checkbox(self):
        option_names = (
            "access_comm_tract_survey_ci",
            "access_comm_zcta_place_survey_ci",
            "access_comm_county_survey_ci",
        )
        for option_name in option_names:
            with self.subTest(option_name=option_name):
                row = {
                    "dentist_pct": 65.0,
                    "dentist_ci_lower": 62.0,
                    "dentist_ci_upper": 68.0,
                }
                _apply_display_option_contract(row, ["dentist"], [option_name])
                self.assertIn("dentist_ci_lower", row)
                self.assertIn("dentist_ci_upper", row)

    def test_fr42_age_adjusted_column_follows_checkbox(self):
        unchecked = {
            "routine_checkup_pct": 72.0,
            "routine_checkup_age_adjusted_pct": 71.0,
        }
        _apply_display_option_contract(unchecked, ["routine_checkup"], [])
        self.assertNotIn("routine_checkup_age_adjusted_pct", unchecked)

        checked = {
            "routine_checkup_pct": 72.0,
            "routine_checkup_age_adjusted_pct": 71.0,
        }
        _apply_display_option_contract(
            checked,
            ["routine_checkup"],
            ["access_comm_county_survey_age_adjusted"],
        )
        self.assertEqual(checked["routine_checkup_age_adjusted_pct"], 71.0)

    def test_fr46_through_fr49_community_ci_follows_section_checkbox(self):
        section_cases = (
            ("limited_english_pct", "limited_english_ci_lower", "limited_english_ci_upper", "community_extended_ci"),
            ("poverty_pct", "poverty_ci_lower", "poverty_ci_upper", "community_economic_ci"),
            ("renting_pct", "renting_ci_lower", "renting_ci_upper", "community_housing_ci"),
            ("moved_last_year", "moved_last_year_ci_lower", "moved_last_year_ci_upper", "community_household_ci"),
        )
        for token, low_key, high_key, option_name in section_cases:
            with self.subTest(token=token):
                unchecked = {low_key: 10.0, high_key: 20.0}
                _apply_display_option_contract(unchecked, [token], [])
                self.assertNotIn(low_key, unchecked)
                self.assertNotIn(high_key, unchecked)

                checked = {low_key: 10.0, high_key: 20.0}
                _apply_display_option_contract(checked, [token], [option_name])
                self.assertIn(low_key, checked)
                self.assertIn(high_key, checked)

    def test_component_community_ci_is_removed_when_not_requested(self):
        row = {
            "employment_employed_pct": 62.0,
            "employment_employed_ci_lower": 60.0,
            "employment_employed_ci_upper": 64.0,
            "occupation_service_pct": 18.0,
            "occupation_service_ci_lower": 16.0,
            "occupation_service_ci_upper": 20.0,
        }
        _apply_display_option_contract(
            row,
            ["employment_16plus", "occupation_dist"],
            [],
        )
        self.assertEqual(
            row,
            {
                "employment_employed_pct": 62.0,
                "occupation_service_pct": 18.0,
            },
        )
