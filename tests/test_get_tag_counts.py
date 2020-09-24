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

    @mock.patch('pandas.DataFrame.to_csv')
    def test_get_tag_counts_pos(self, mock_to_csv):
        """
        Positive test case for the get_tag_counts function.
        """
        extractor = XbrlExtraction()

        df = self.input_data()

        extractor.get_tag_counts(df, "name", "output_folder", "month", "year")

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
        # Assert
        #with self.assertRaises(TypeError):
        #    subsets.tag_extraction(1.0, 'Name', 'A')

    def test_get_tag_counts_values(self):
        """
        Values test case for the get_tag_counts function.
        """
        # Assert
        #with self.assertRaises(ValueError):
        #    subsets.tag_extraction(dataframe, 'Surname', 'A','2012', "February")
        #Need valueError tests for folder name, month and year
        #check year with integer value 2
