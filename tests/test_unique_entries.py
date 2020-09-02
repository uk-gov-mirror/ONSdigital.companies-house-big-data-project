import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlSubsets


class TestUniqueEntries(unittest.TestCase):
    """

    """
    def test_unique_entries_pos(self):
        """
        Positive test case for the unique_entries function.
        """
        # Dataframe that we create.
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30],
                            [4, 5, 12, 8, 22],
                            [4, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])

        # The dataframe the function should return (tp_unique_entries1)
        df2 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])

        # The list the function should return (tp_unique_entries2)
        list1 = [1, 4, 7]

        # Assume
        subsets = XbrlSubsets()

        # Assume 1
        tp_unique_entries1 = subsets.unique_entries(df1, 'A', False)
        # Assume 2
        tp_unique_entries2 = subsets.unique_entries(df1, 'A', True)

        # Assert 1
        assert_frame_equal(tp_unique_entries1.reset_index(drop=True),
                           df2.reset_index(drop=True))
        # Assert 2
        self.assertListEqual(tp_unique_entries2, list1)

    def test_unique_entries_neg(self):
        """
        Negative test case for the unique_entries function.
        """

        # Dataframe that we create.
        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30],
                            [4, 5, 12, 8, 22],
                            [4, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])

        # Dataframe that is NOT the same as the one the function should return.
        df2 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 12, 8, 22],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])

        # List that is NOT the same as the one the function should return.
        list1 = [1, 4, 4, 4, 7]

        # Assume
        subsets = XbrlSubsets()

        # Assume 1
        tn_unique_entries1 = subsets.unique_entries(df1, 'A', False)
        # Assume 2
        tn_unique_entries2 = subsets.unique_entries(df1, 'A', True)

        # Assert 1
        self.assertNotEqual(tn_unique_entries1.reset_index(drop=True).equals(df2.reset_index(drop=True)), True)
        # Assert 2
        self.assertNotEqual(tn_unique_entries2 == list1, True)

    def test_types(self):
        """
        Positive test case for the unique_entries function.
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
            subsets.unique_entries(1.0, 'A', False)

        with self.assertRaises(TypeError):
            subsets.unique_entries(df1, None, False)

        with self.assertRaises(TypeError):
            subsets.unique_entries(df1, ['A', 'B'], True)

        with self.assertRaises(TypeError):
            subsets.unique_entries(df1, 'A', 'False')

    def test_values(self):
        """
        Positive test case for the unique_entries function.
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
            subsets.unique_entries(df1, 'I', False)

        with self.assertRaises(ValueError):
            subsets.unique_entries(df1, 'A,B', True)
