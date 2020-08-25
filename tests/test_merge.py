import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

# Custom import
from src.xbrl_processing.merge import merge


class TestMerge(unittest.TestCase):
    """

    """
    def test_merge_pos(self):
        """
        Positive test case for the merge function.
        """

        df1 = pd.DataFrame([['A', '10'],
                            ['B', '20']],
                           columns=['Name', 'Age'])
        df2 = pd.DataFrame([['A', 'football'],
                            ['G', 'swimming']],
                           columns=['Name', 'Sport'])
        df3 = pd.DataFrame([['A', '10', 'football']],
                           columns=['Name', 'Age', 'Sport'])

        # Assume
        tp_merge = merge(df1, df2, 'Name', 'Name', 'inner', 'df1')

        # Assert
        assert_frame_equal(tp_merge, df3)

    def test_merge_neg(self):
        """
        Negative test case for the merge function.
        """

        df1 = pd.DataFrame([['A', '10'],
                            ['B', '20']],
                           columns=['Name', 'Age'])
        df2 = pd.DataFrame([['A', 'football'],
                            ['G', 'swimming']],
                           columns=['Name', 'Sport'])
        df3 = pd.DataFrame([['A', '20', 'football']],
                           columns=['Name', 'Age', 'Sport'])

        # Assume
        tp_merge = merge(df1, df2, 'Name', 'Name', 'inner', 'df1')

        # Assert
        self.assertEqual(tp_merge.equals(df3), False)

    def test_types(self):
        """
        Positive test case for the merge function.
        """

        # Assume
        df4 = pd.DataFrame([['A', '10'],
                            ['B', '20']],
                           columns=['Name', 'Age'])
        df5 = pd.DataFrame([['A', 'football'],
                            ['G', 'swimming']],
                           columns=['Name', 'Sport'])

        # Assert
        with self.assertRaises(TypeError):
            merge(df4, df5, False, 'Name')

        with self.assertRaises(TypeError):
            merge(df4, df5, ['Name', 'Age'], 'Name')

        with self.assertRaises(TypeError):
            merge('Name', df5, 'Name', 'Name')

        with self.assertRaises(TypeError):
            merge(df4, 'df5', 'Name', 'Name')

    def test_values(self):
        """
        Positive test case for the merge function.
        """

        # Assume
        df4 = pd.DataFrame([['A', '10'],
                            ['B', '20']],
                           columns=['Name', 'Age'])
        df5 = pd.DataFrame([['A', 'football'],
                            ['G', 'swimming']],
                           columns=['Name', 'Sport'])

        # Assert
        with self.assertRaises(ValueError):
            merge(df4, df5, 'Name', 'Name', 'merge')

        with self.assertRaises(ValueError):
            merge(df4, df5, 'Name', 'Name', 'inner', 'inner')

        with self.assertRaises(ValueError):
            merge(df4, df5, 'Name', 'Name', 'both', 'inner')

        with self.assertRaises(ValueError):
            merge(df4, df5, 'Name', 'Name', 'both')
