import pandas
import unittest
import unittest.mock as mock
from pandas.testing import assert_frame_equal

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlExtraction


class TestOutputXbrlMonth(unittest.TestCase):
    """
    """
    def data_input(self):

        df = pandas.DataFrame([['A', '10'],
                              ['B', '20'],
                              ['A', '30'],
                              ['C', '40'],
                              ['D', '50'],
                              ['C', '60']],
                              columns=['name', 'Age'])

        return df

    @mock.patch('os.path')
    @mock.patch('pandas.DataFrame.to_csv')
    def test_output_xbrl_month_pos(self, mock_to_csv, mock_path):
        """
        Positive test case for the output_xbrl_month function.
        """
        extractor = XbrlExtraction()
        # mock_path.exists.return_value = True
        df = self.data_input()

        extractor.output_xbrl_month(df, "output_folder", "January", "2010", "csv")

        self.assertTrue(mock_to_csv.called, "CSV file not saved")

        self.assertTrue(mock_to_csv, "output_folder" + "/" + "folder_year"
                        + "-" + "folder_month" + "_xbrl_data.csv")

    def test_output_xbrl_month_types(self):
        """
        Types test case for the output_xbrl_month function.
        """

        extractor = XbrlExtraction()
        df = self.data_input()

        with self.assertRaises(TypeError):
            extractor.output_xbrl_month(1.0, "output_folder", "month", "year", "file_type")

        with self.assertRaises(TypeError):
            extractor.output_xbrl_month(df, 1, "month", "year", "file_type")

        with self.assertRaises(TypeError):
            extractor.output_xbrl_month(df, "output_folder", 1, "year", "file_type")

        with self.assertRaises(TypeError):
            extractor.output_xbrl_month(df, "output_folder", "month", 1, "file_type")

        with self.assertRaises(TypeError):
            extractor.output_xbrl_month(df, "name", "output_folder", "month", 1)

    def test_output_xbrl_month_values(self):
        """
        Values test case for the output_xbrl_month function.
        """
        extractor = XbrlExtraction()
        df = self.data_input()

        with self.assertRaises(ValueError):
            extractor.output_xbrl_month(df, "notafolder", "January", "year", "file_type")

        with self.assertRaises(ValueError):
            extractor.output_xbrl_month(df, "output_folder", "notamonth", "year", "file_type")

        with self.assertRaises(ValueError):
            extractor.output_xbrl_month(df, "output_folder", "January", "notayear", "file_type")


if __name__ == "__main__":
    unittest.main(verbosity=2)

