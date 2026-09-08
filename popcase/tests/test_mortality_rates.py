from unittest.mock import patch
from django.test import SimpleTestCase
from popcase.mortality_rates import cancer_death_matches, death_ages
from popcase.incidence_rates import subcounty_incidence
from popcase.rate_statistics import AGE_BANDS_20, RateDataUnavailable


class MortalityTests(SimpleTestCase):
    filters = {'dx_start': '2022q1', 'dx_end': '2022q4'}
    row = ('patient', '0', '20220301', '19720302', 'C159', '1', 'C150', '8140')

    def test_cause_matches_cancer_not_just_deceased_status(self):
        self.assertTrue(cancer_death_matches('C15.9', '1', 'C150', '8140'))
        self.assertTrue(cancer_death_matches('C159', '', 'C150', '8140'))
        self.assertFalse(cancer_death_matches('C509', '1', 'C150', '8140'))
        self.assertFalse(cancer_death_matches('I219', '1', 'C150', '8140'))
        for cause, revision in [('', '1'), (None, '1'), ('7777', '1'), ('7797', '1'), ('1509', '9')]:
            self.assertIsNone(cancer_death_matches(cause, revision, 'C150', '8140'))

    def test_death_year_and_age_not_diagnosis_year_and_age(self):
        self.assertEqual(death_ages([self.row], self.filters, 'tract', AGE_BANDS_20), {'patient': 49})
        self.assertEqual(death_ages([self.row], {'dx_start': '2021', 'dx_end': '2021'}, 'tract', AGE_BANDS_20), {})

    def test_blank_death_data_are_unavailable_not_zero(self):
        for index in (1, 2, 4):
            row = list(self.row)
            row[index] = ''
            with self.assertRaises(RateDataUnavailable):
                death_ages([row], self.filters, 'tract', AGE_BANDS_20)
        alive = list(self.row)
        alive[1], alive[4] = '1', ''
        self.assertEqual(death_ages([alive], self.filters, 'tract', AGE_BANDS_20), {})

    def test_county_and_subcounty_mortality_outputs(self):
        for level, geo in [('county', '39001'), ('tract', '39001000100'), ('place', '3900100'), ('zcta', '43001')]:
            band = (45, 49)
            with self.subTest(level=level), patch('popcase.incidence_rates.validate_tract_rate_period'), \
                 patch('popcase.incidence_rates.load_case_counts', return_value=({geo: 2}, {geo: {band: 2}}, {band: 10}, {})) as counts, \
                 patch('popcase.incidence_rates.load_target_populations', return_value=({geo: {band: 1000}}, {})), \
                 patch('popcase.mortality_rates.load_county_population', return_value=({geo: {band: 1000}}, {})), \
                 patch('popcase.incidence_rates.load_ohio_annual_population', return_value={band: 1000}), \
                 patch('popcase.incidence_rates.load_ohio_decennial_population', return_value={band: 1000}):
                row = subcounty_incidence('2022', level, self.filters, mortality=True)[0]
                self.assertTrue(counts.call_args.kwargs['mortality'])
                self.assertEqual(row['crude_incidence_per_100k'], 200)
                self.assertIsNotNone(row['crude_incidence_ci_upper'])
                self.assertEqual(row['age_adjusted_per_100k'], 200 if level == 'county' else 40)
                if level == 'county':
                    self.assertIsNone(row['age_adjusted_ci_upper'])
                else:
                    self.assertGreater(row['age_adjusted_ci_upper'], 40)
