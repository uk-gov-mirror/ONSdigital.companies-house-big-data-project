import unittest
<<<<<<< HEAD
import pandas
import unittest.mock as mock
=======
import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import Mock
from unittest.mock import patch
>>>>>>> 3c95e861b9f940374cfd5b50861fc30f58fb14d3

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlExtraction

<<<<<<< HEAD
class RetrieveListOfTags(unittest.TestCase):
    def input_data(self):

        df = pandas.DataFrame([['A', '10'],
=======

class TestTagExtraction(unittest.TestCase):
    """

    """
    #@patch("home/lewysel/repos/companies_house_accounts/src/xbrl_pd_methods.py.list_of_tags_unique")

    def test_retrieve_list_of_tags(self, list_mock):
        """
        Positive test case for the retrieve_list_of_tags function.
        Which tests for the correct list being produced
        """

        # Dataframe that we create.
        df = pd.DataFrame([['A', '10'],
>>>>>>> 3c95e861b9f940374cfd5b50861fc30f58fb14d3
                            ['B', '20'],
                            ['A', '30'],
                            ['C', '40'],
                            ['D', '50'],
                            ['C', '60']],
<<<<<<< HEAD
                           columns=['name', 'Age'])

        return df
    '''
    
    '''
    @mock.patch('builtins.open')
    def test_retrieve_list_of_tags_pos(self,mock_open):
        '''
        Test to check if the open function is called

        :param mock_open:

        :return:

        '''
        #define test variables
        df = self.input_data()
        column = 'name'


        mock_file_name = 'content_folder/2010-January_list_of_tags.txt'

        with mock.patch('builtins.open',mock_open()) as mocked_file:
            XbrlExtraction().retrieve_list_of_tags(df,'name','content_folder','January','2010')
            mocked_file.assert_called_once_with(mock_file_name,'w')


        #extractor.retrieve_list_of_tags(df,column, "output_folder", "January", "2010")

        #check if open has been called
        #self.assertTrue(mock_open.called,"Text file not outputted")




if __name__ == '__main__':
    unittest.main()
=======
                           columns=['name', 'value'])


        # The list the function should produce
        expected_list = ['A','B','C','D']


        # Assume
        subsets = XbrlExtraction()

        # Assume 1
        tp_tag_extraction1 = subsets.retrieve_list_of_tags(df, 'Nhome/lewysel/repos/companies_house_accounts/tests/test_outputs/retrieve_list_of_tags', 'Month','Year')

        # Assert 1
        assert_frame_equal(tp_tag_extraction1.reset_index(drop=True),
                           df2.reset_index(drop=True))
        # Assert 2
        assert_frame_equal(tp_tag_extraction2.reset_index(drop=True),
                           df3.reset_index(drop=True))
'''
    def test_tag_extraction_neg(self):
        """
        Negative test case for the tag_extraction function.
        """

        # Dataframe that we create.
        df1 = pd.DataFrame([['A', '10'],
                            ['B', '20'],
                            ['A', '30'],
                            ['C', '40'],
                            ['D', '50'],
                            ['C', '60']],
                           columns=['Name', 'Age'])

        # Dataframe that is NOT the same as the one the function should return.
        df2 = pd.DataFrame([['10'],
                            ['30']],
                           columns=['Age'])

        # Dataframe that is NOT the same as the one the function should return.
        df3 = pd.DataFrame([['A', '10'],
                            ['C', '40'],
                            ['A', '30'],
                            ['C', '60']],
                           columns=['Name', 'Age'])

        # Assume
        subsets = XbrlSubsets()

        # Assume 1
        tn_tag_extraction1 = subsets.tag_extraction(df1, 'Name', 'A')
        # Assume 2
        tn_tag_extraction2 = subsets.tag_extraction(df1, 'Name', ['A', 'C'])

        # Assert 1
        self.assertNotEqual(tn_tag_extraction1.reset_index(drop=True).equals(df2.reset_index(drop=True)), True)
        # Assert 2
        self.assertNotEqual(tn_tag_extraction2.reset_index(drop=True).equals(df2.reset_index(drop=True)), True)

    def test_types(self):
        """
        Positive test case for the tag_extraction function.
        """
        # Assume
        df1 = pd.DataFrame([['A', '10'],
                            ['B', '20'],
                            ['A', '30'],
                            ['C', '40'],
                            ['D', '50']],
                           columns=['Name', 'Age'])
        # Assume
        subsets = XbrlSubsets()

        # Assert
        with self.assertRaises(TypeError):
            subsets.tag_extraction(1.0, 'Name', 'A')

        with self.assertRaises(TypeError):
            subsets.tag_extraction(df1, 1, 'A')

        with self.assertRaises(TypeError):
            subsets.tag_extraction(df1, 'Name', {'A': 'a'})

        with self.assertRaises(TypeError):
            subsets.tag_extraction(df1, 'Name', all)

    def test_values(self):
        """
        Positive test case for the tag_extraction function.
        """
        # Assume
        df1 = pd.DataFrame([['A', '10'],
                            ['B', '20'],
                            ['A', '30'],
                            ['C', '40'],
                            ['D', '50']],
                           columns=['Name', 'Age'])
        # Assume
        subsets = XbrlSubsets()

        # Assert
        with self.assertRaises(ValueError):
            subsets.tag_extraction(df1, 'names', 'A')

        with self.assertRaises(ValueError):
            subsets.tag_extraction(df1, 'A', 'Name')
'''
>>>>>>> 3c95e861b9f940374cfd5b50861fc30f58fb14d3
