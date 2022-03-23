
from data_processing import group_multdoses, make_summary_df, make_ade_df
import pandas as pd
import unittest

def test_group_multdoses():
    #in case more options added in future
    actual = group_multdoses('8')
    expected = '5+'
    assert actual == expected

#referenced Metis unittest exercise
class TestSummaryMaker(unittest.TestCase):
    def test_fails_no_df(self):
        with self.assertRaises(TypeError):
            make_summary_df()
    def test_admin_location_specified(self):
        summary_df = make_summary_df(df)
        self.assertTrue('PHM' not in list(summary_df.v_adminby.values))
    def test_higher_dose_grouped(self):
        summary_df = make_summary_df(df)
        self.assertTrue('6' not in list(summary_df.vax_dose_series.values))

class TestAdeMaker(unittest.TestCase):
    def test_fails_no_df(self):
        with self.assertRaises(TypeError):
            make_ade_df()
    def test_no_nulls(self):
        summary_df = make_summary_df(df)
        self.assertFalse(summary_df.vax_manu.isnull().values.any())
        self.assertFalse(summary_df.vax_dose_series.isnull().values.any())
