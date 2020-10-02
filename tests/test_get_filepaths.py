import pandas
import unittest
import unittest.mock as mock
from pandas.testing import assert_frame_equal

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlExtraction

class TestGetFilepaths(unittest.TestCase):
    """

    """

    @mock.patch('os.listdir')
    def test_get_filepaths_pos(self, mock_listdir):
        """
        Positive test case for the get_filepaths function.
        """
        extractor = XbrlExtraction()

        # Accounts_Monthly_Data-December2014/Prod224_0013_07971828_20140331.html
        file = "Prod224_0013_07971828_20140331.html"
        directory = "Accounts_Monthly_Data-December2014"
        mock_listdir.return_value = [file]

        files, month, year = extractor.get_filepaths(directory)

        self.assertTrue(files == [directory + "/" + file], "Directory not present")
        self.assertTrue(month == "December", "Incorrect month")
        self.assertTrue(year == "2014", "Incorrect year")

    @mock.patch('os.listdir')
    def test_get_filepaths_neg(self, mock_listdir):
        """
        Negative test case for the get_filepaths function.

        Is this test needed?
        """
        extractor = XbrlExtraction()

        # Accounts_Monthly_Data-December2014/Prod224_0013_07971828_20140331.html
        file = "Prod224_0013_07971828_20140331.html"
        directory = "Accounts_Monthly_Data-December2014"
        mock_listdir.return_value = [file]

        files, month, year = extractor.get_filepaths(directory)

        self.assertFalse(files != [directory + "/" + file], "Directory not present")
        self.assertFalse(month != "December", "Incorrect month")
        self.assertFalse(year != "2014", "Incorrect year")


    #@mock.patch('os.listdir')
    def test_get_filepaths_types(self):
        """
        Types test case for the get_filepaths function.
        """

        extractor = XbrlExtraction()

        # file = "Prod224_0013_07971828_20140331.html"
        # directory = "Accounts_Monthly_Data-December2014"
        # mock_listdir.return_value = [file]
        #int
        with self.assertRaises(TypeError):
            extractor.get_filepaths(1)
        #list
        with self.assertRaises(TypeError):
            extractor.get_filepaths(["Accounts_Monthly_Data-December2014"])
        #tuple
        with self.assertRaises(TypeError):
            extractor.get_filepaths(("Accounts_Monthly_Data-December2014","Accounts_Monthly_Data-December2015"))
if __name__ == '__main__':
    unittest.main()