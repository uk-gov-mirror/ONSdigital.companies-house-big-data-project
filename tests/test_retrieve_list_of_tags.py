import unittest
import pandas
import unittest.mock as mock
import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import Mock
from unittest.mock import patch

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlExtraction

class RetrieveListOfTags(unittest.TestCase):

    def input_data(self):
        '''

        :return:
        '''
        # Dataframe that we create.
        df = pd.DataFrame([['A', '10'],
                           ['B', '20'],
                           ['A', '30'],
                           ['C', '40'],
                           ['D', '50'],
                           ['C', '60']],
                          columns=['name', 'Age'])
        return df

    @mock.patch('builtins.open')
    def test_retrieve_list_of_tags_pos(self,mock_open):
        '''
        Test to check if the open function is called and that the file name and directory
        is as expected. Additionally checks the file is in write mode.

        :param mock_open: mocked instance of the builtin python open method

        :return: None

        '''

        #define test variables
        df = self.input_data()
        column = 'name'
        folder = '/shares/xbrl_parsed_data'
        month = 'January'
        year = '2010'

        mock_file_name = folder+'/'+year+'-'+month+'_list_of_tags.txt'

        with mock.patch('builtins.open',mock_open()) as mocked_file:
            XbrlExtraction().retrieve_list_of_tags(df,column,folder,month,year)
            mocked_file.assert_called_once_with(mock_file_name,'w')

    def test_retrieve_list_of_tags_type(self):
        '''

        :return:
        '''
        extractor = XbrlExtraction()
        df = self.input_data()

        #check if dataframe = string
        with self.assertRaises(TypeError):
            extractor.retrieve_list_of_tags('df', 'name', 'output_folder', 'January', '2010')

        #check type error for column = list type
        with self.assertRaises(TypeError):
            extractor.retrieve_list_of_tags(df,['name'],'output_folder','January','2010')

        #check type error for folder = list
        with self.assertRaises(TypeError):
            extractor.retrieve_list_of_tags(df, 'name', ['output_folder'], 'January', '2010')

        #check type error for month = int
        with self.assertRaises(TypeError):
            extractor.retrieve_list_of_tags(df, 'name', 'output_folder', 1, '2010')

        # check type error for year = int
        with self.assertRaises(TypeError):
            extractor.retrieve_list_of_tags(df, 'name', 'output_folder', 'January', 2010)

    def test_retrieve_list_of_tags_value(self):
        '''

        :return:
        '''
        extractor = XbrlExtraction()

        #define test variables
        df = self.input_data()
        column = 'name'
        folder = '/shares/xbrl_parsed_data'
        month = 'January'
        year = '2010'


        # check if column not in dataframe raises error
        with self.assertRaises(ValueError):
            extractor.retrieve_list_of_tags(df, 'potatoe', folder, month, year)

        # check value error for invalid directory
        with self.assertRaises(ValueError):
            extractor.retrieve_list_of_tags(df, column, 'not_real_directory',  month, year)

        # check value error for invalid month
        with self.assertRaises(ValueError):
            extractor.retrieve_list_of_tags(df, column, folder, 'Jan', year)

        # check value error if year not also int
        with self.assertRaises(ValueError):
            extractor.retrieve_list_of_tags(df, column, folder, month, '2k10')

#Is this bit needed?
if __name__ == '__main__':
    unittest.main()
