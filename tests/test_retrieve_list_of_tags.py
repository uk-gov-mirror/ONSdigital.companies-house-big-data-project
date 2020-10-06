import unittest
import pandas
import unittest.mock as mock

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlExtraction

class RetrieveListOfTags(unittest.TestCase):
    def input_data(self):

        df = pandas.DataFrame([['A', '10'],
                            ['B', '20'],
                            ['A', '30'],
                            ['C', '40'],
                            ['D', '50'],
                            ['C', '60']],
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
