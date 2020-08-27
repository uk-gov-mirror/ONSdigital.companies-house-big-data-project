import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlSubsets


class TestAggregation(unittest.TestCase):
    """

    """
    def test_aggregation_pos(self):
        """
        Positive test case for the aggregation function.
        """
        # Dataframe that we create.
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30],
                            [4, 5, 12, 8, 22],
                            [4, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])

        # The dataframe that the function should return (tp_aggregation1)
        df2 = pd.DataFrame([[1, 3, 19],
                            [4, 6, 30],
                            [7, 12, 5]],
                           columns=['A', 'first  D', 'first  E'])
        df2 = pd.DataFrame(df2).set_index('A')

        # The dataframe that the function should return (tp_aggregation2)
        df3 = pd.DataFrame([[1, 3, 19],
                            [4, 6, 30],
                            [7, 12, 5]],
                           columns=['A', 'D', 'E'])
        df3 = pd.DataFrame(df3).set_index('A')

        # Assume
        subsets = XbrlSubsets()

        # Assume 1
        tp_aggregation1 = subsets.aggregation(df1, ['A'], 'first', ['D', 'E'], False)
        # Assume 2
        tp_aggregation2 = subsets.aggregation(df1, ['A'], 'first', ['D', 'E'], True)

        # Assert 1
        assert_frame_equal(tp_aggregation1, df2)
        # Assert 2
        assert_frame_equal(tp_aggregation2, df3)

    def test_aggregation_neg(self):
        """
        Negative test case for the aggregation function.
        """
        # Dataframe that we create.
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30],
                            [4, 5, 12, 8, 22],
                            [4, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])

        # Dataframe that is NOT the same as the one the function should return.
        df2 = pd.DataFrame([[1, 3, 19],
                            [4, 6, 30],
                            [7, 12, 6]],
                           columns=['A', 'first  D', 'first  E'])
        df2 = pd.DataFrame(df2).set_index('A')

        # Dataframe that is NOT the same as the one the function should return.
        df3 = pd.DataFrame([[1, 3, 19],
                            [4, 6, 30],
                            [7, 12, 5]],
                           columns=['A', 'first  D', 'first  E'])
        df3 = pd.DataFrame(df3).set_index('A')

        # Assume
        subsets = XbrlSubsets()

        # Assume 1
        tn_aggregation1 = subsets.aggregation(df1, ['A'], 'first', ['D', 'E'], False)
        # Assume 2
        tn_aggregation2 = subsets.aggregation(df1, ['A'], 'first', ['D', 'E'], True)

        # Assert 1
        self.assertNotEqual(tn_aggregation1.equals(df2), True)
        # Assert 2
        self.assertNotEqual(tn_aggregation2.equals(df3), True)

    def test_types(self):
        """
        Positive test case for the aggregation function.
        """
        # Assume
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30],
                            [4, 5, 12, 8, 22],
                            [4, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])
        # Assume
        subsets = XbrlSubsets()

        # Assert
        with self.assertRaises(TypeError):
            subsets.aggregation(df1, 'A', 'sum', 'B', 'False')

        with self.assertRaises(TypeError):
            subsets.aggregation(1, 'A', 'sum', 'B', False)

        with self.assertRaises(TypeError):
            subsets.aggregation(df1, {'A': 'a'}, 'sum', 'B', False)

        with self.assertRaises(TypeError):
            subsets.aggregation(df1, 'A', 'sum', 6, True)

    def test_values(self):
        """
        Positive test case for the aggregation function.
        """

        # Assume
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30],
                            [4, 5, 12, 8, 22],
                            [4, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])
        # Assume
        subsets = XbrlSubsets()

        # Assert
        with self.assertRaises(ValueError):
            subsets.aggregation(df1, 'A', 'summation', 'B', False)

        with self.assertRaises(ValueError):
            subsets.aggregation(df1, 'A', sum, 'B', False)
