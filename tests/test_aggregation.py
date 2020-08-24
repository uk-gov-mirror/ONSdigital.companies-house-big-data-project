import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal

# Custom import
from src.xbrl_processing.aggregation import aggregation

class TestAggregation(unittest.TestCase):
    
    def test_aggregation_pos(self):
        """
        Positive test case for the aggregation function.
        """
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                   [4, 5, 8, 6, 30],
                   [4, 5, 12, 8, 22],
                   [4, 7, 9, 5, 21],
                   [7, 8, 9, 12, 5]],
                  columns=['A', 'B', 'C', 'D', 'E'])
        df2 = pd.DataFrame([[1,3,19],
                           [4,6,30],
                           [7,12,5]],
                          columns=['A','first  D', 'first  E'])
        df2 = pd.DataFrame(df2).set_index('A')
        
        tp_aggregation = aggregation(df1, ['A'], 'first', ['D', 'E'], False)
        assert_frame_equal(tp_aggregation, df2)
        
    def test_aggregation_neg(self):
        """
        Negative test case for the aggregation function.
        """
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                   [4, 5, 8, 6, 30],
                   [4, 5, 12, 8, 22],
                   [4, 7, 9, 5, 21],
                   [7, 8, 9, 12, 5]],
                  columns=['A', 'B', 'C', 'D', 'E'])
        # Assume
        df2 = pd.DataFrame([[1,3,19],
                           [4,6,30],
                           [7,12,6]],
                          columns=['A','first  D', 'first  E'])
        df2 = pd.DataFrame(df2).set_index('A')
        
        tp_aggregation = aggregation(df1, ['A'], 'first', ['D', 'E'], False)
        self.assertEqual(df2.equals(tp_aggregation), False)

        
    def test_types(self):
        """
        Positive test case for the aggregation function.
        """        
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                   [4, 5, 8, 6, 30],
                   [4, 5, 12, 8, 22],
                   [4, 7, 9, 5, 21],
                   [7, 8, 9, 12, 5]],
                  columns=['A', 'B', 'C', 'D', 'E'])
        
        with self.assertRaises(TypeError):
            aggregation(df1, 'A', 'sum', 'B', 'False')
            
        with self.assertRaises(TypeError):
            aggregation(1, 'A', 'sum', 'B', False)

        with self.assertRaises(TypeError):
            aggregation(df1, {'A':'a'}, 'sum', 'B', False)       

        with self.assertRaises(TypeError):
            aggregation(df1, 'A', 'sum', 6 , False)   
            
            
    def test_values(self):
        """
        Positive test case for the aggregation function.
        """
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                   [4, 5, 8, 6, 30],
                   [4, 5, 12, 8, 22],
                   [4, 7, 9, 5, 21],
                   [7, 8, 9, 12, 5]],
                  columns=['A', 'B', 'C', 'D', 'E'])
        
        with self.assertRaises(ValueError):
            aggregation(df1, 'A', 'summ', 'B', False)
            
        with self.assertRaises(ValueError):
            aggregation(df1, 'A', sum, 'B', False)