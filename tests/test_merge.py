import unittest
import pandas as pd
#import pandas.testing.assert_frame_equal
from pandas.util.testing import assert_frame_equal
# Custom import
from src.xbrl_processing.merge import merge


class TestMerge(unittest.TestCase):
    """

    """
    def test_merge_pos(self):
        """
        Positive test case for the merge function.
        """

        df1 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30],
                            [4, 5, 12, 8, 22],
                            [4, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5]],
                           columns=['A', 'B', 'C', 'D', 'E'])

        df2 = pd.DataFrame([[1, 6, 2, 3, 19],
                            [5, 5, 8, 6, 30],
                            [9, 5, 12, 8, 22],
                            [4, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5]],
                           columns=['Ab', 'B', 'C', 'D', 'E'])

        df3 = pd.DataFrame([[1, 6, 2, 3, 19, 6, 2, 3, 19],
                            [4, 5, 8, 6, 30, 7, 9, 5, 21],
                            [4, 5, 12, 8, 22, 7, 9, 5, 21],
                            [4, 7, 9, 5, 21, 7, 9, 5, 21],
                            [7, 8, 9, 12, 5, 8, 9, 12, 5]],
                           columns=['A', 'B_x', 'C_x', 'D_x', 'E_x', 'B_y', 'C_y', 'D_y', 'E_y'])

        df4 = pd.DataFrame([['A', '10'],
                            ['B', '20']],
                           columns=['Name', 'Age'])
        df5 = pd.DataFrame([['A', 'football'],
                            ['G', 'swimming']],
                           columns=['Name', 'Sport'])
        df6 = pd.DataFrame([['A', '10', 'football']],
                           columns=['Name', 'Age', 'Sport'])
        # Assume
        tp_merge = merge(df4, df5, 'Name', 'Name', 'inner', 'df1')

        # Assert
        assert_frame_equal(tp_merge, df6)

    def test_merge_neg(self):
        """
        Negative test case for the merge function.
        """
        df4 = pd.DataFrame([['A', '10'],
                            ['B', '20']],
                           columns=['Name', 'Age'])
        df5 = pd.DataFrame([['A', 'football'],
                            ['G', 'swimming']],
                           columns=['Name', 'Sport'])
        df6 = pd.DataFrame([['A', '20', 'football']],
                           columns=['Name', 'Age', 'Sport'])
        # Assume
        tp_merge = merge(df4, df5, 'Name', 'Name', 'inner', 'df1')

        # Assert
        self.assertEqual(tp_merge.equals(df6), False)

    def test_types(self):
        """
        Positive test case for the merge function.
        """

        df4 = pd.DataFrame([['A', '10'],
                            ['B', '20']],
                           columns=['Name', 'Age'])
        df5 = pd.DataFrame([['A', 'football'],
                            ['G', 'swimming']],
                           columns=['Name', 'Sport'])
        df6 = pd.DataFrame([['A', '20', 'football']],
                           columns=['Name', 'Age', 'Sport'])

        # Assert
        with self.assertRaises(TypeError):
            merge(df4, df5, False, 'Name')

        with self.assertRaises(TypeError):
            merge(df4, df5, ['Name', 'Age'], 'Name')

        with self.assertRaises(TypeError):
            merge('Name', df5, 'Name', 'Name')

    def test_values(self):
        """
        Positive test case for the merge function.
        """

        df4 = pd.DataFrame([['A', '10'],
                            ['B', '20']],
                           columns=['Name', 'Age'])
        df5 = pd.DataFrame([['A', 'football'],
                            ['G', 'swimming']],
                           columns=['Name', 'Sport'])
        df6 = pd.DataFrame([['A', '20', 'football']],
                           columns=['Name', 'Age', 'Sport'])

        # Assert

        with self.assertRaises(ValueError):
            merge(df4, df5, 'Name', 'Name', 'innerr')

        with self.assertRaises(ValueError):
            merge(df4, df5, 'Name', 'Name', 'inner', 'inner')

        with self.assertRaises(ValueError):
            merge(df4, df5, 'Name', 'Name', 'both', 'inner')

