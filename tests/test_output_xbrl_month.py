import pandas as pd
import unittest
import unittest.mock as mock

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlExtraction


class TestOutputXbrlMonth(unittest.TestCase):
    """
    """
    def data_input(self):

        df = pd.DataFrame([['A', '10'],
                           ['B', '20'],
                           ['A', '30'],
                           ['C', '40'],
                           ['D', '50'],
                           ['C', '60']],
                          columns=['name', 'Age'])

        return df

    @mock.patch('os.path')
    @mock.patch('pd.DataFrame.to_csv')
    def test_output_xbrl_month_pos(self, mock_to_csv, mock_path):
        """
        Positive test case for the output_xbrl_month function.
        """
        extractor = XbrlExtraction()
        mock_path.exists.return_value = True
        df = self.data_input()

        extractor.output_xbrl_month(df, "output_folder", "January", "2010")

        self.assertTrue(mock_to_csv.called, "CSV file not saved")
