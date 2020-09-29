import pandas
import unittest
import unittest.mock as mock
from pandas.testing import assert_frame_equal

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlExtraction

class TestGetTagCounts(unittest.TestCase):
    """

    """

    def input_data(self):

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
    def test_get_tag_counts_pos(self, mock_to_csv, mock_path):
        """
        Positive test case for the get_tag_counts function.
        """
        extractor = XbrlExtraction()
        mock_path.exists.return_value = True
        df = self.input_data()

        extractor.get_tag_counts(df, "name", "output_folder", "January", "2010")

        self.assertTrue(mock_to_csv.called, "CSV file not saved")

    def test_get_tag_counts_neg(self):
        """
        Negative test case for the get_tag_counts function.

        Is this test needed?
        """
        pass

    def test_get_tag_counts_types(self):
        """
        Types test case for the get_tag_counts function.
        """

        extractor = XbrlExtraction()
        df = self.input_data()

        with self.assertRaises(TypeError):
            extractor.get_tag_counts(1.0, "name", "output_folder", "month", "year")

        with self.assertRaises(TypeError):
            extractor.get_tag_counts(df, 1, "output_folder", "month", "year")

        with self.assertRaises(TypeError):
            extractor.get_tag_counts(df, "name", 1, "month", "year")

        with self.assertRaises(TypeError):
            extractor.get_tag_counts(df, "name", "output_folder", 1, "year")

        with self.assertRaises(TypeError):
            extractor.get_tag_counts(df, "name", "output_folder", "month", 1)

    def test_get_tag_counts_values(self):
        """
        Values test case for the get_tag_counts function.
        """
        extractor = XbrlExtraction()
        df = self.input_data()

        with self.assertRaises(ValueError):
           extractor.get_tag_counts(df, "three", "output_folder", "month", "year")

        with self.assertRaises(ValueError):
           extractor.get_tag_counts(df, "name", "output_folder", "notamonth", "year")

        with self.assertRaises(ValueError):
           extractor.get_tag_counts(df, "name", "notafolder", "January", "year")

        with self.assertRaises(ValueError):
           extractor.get_tag_counts(df, "name", "output_folder", "January", "notayear")
