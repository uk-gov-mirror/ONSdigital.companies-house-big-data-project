import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

# Custom import
from src.xbrl_processing.tag_extraction import tag_extraction


class TestTagExtraction(unittest.TestCase):
    """

    """
    def test_tag_extraction_pos(self):
        """
        Positive test case for the tag_extraction function.
        """
        # Dataframe that we create.
        df1 = pd.DataFrame([['A', '10'],
                            ['B', '20'],
                            ['A', '30'],
                            ['C', '40'],
                            ['D', '50'],
                            ['C', '60']],
                           columns=['Name', 'Age'])

        # The dataframe the function should return (tp_tag_extraction1).
        df2 = pd.DataFrame([['A', '10'],
                            ['A', '30']],
                           columns=['Name', 'Age'])

        # The dataframe the function should return (tp_tag_extraction2).
        df3 = pd.DataFrame([['A', '10'],
                            ['A', '30'],
                            ['C', '40'],
                            ['C', '60']],
                           columns=['Name', 'Age'])

        # Assume 1
        tp_tag_extraction1 = tag_extraction(df1, 'Name', 'A')
        # Assume 2
        tp_tag_extraction2 = tag_extraction(df1, 'Name', ['A', 'C'])

        # Assert 1
        assert_frame_equal(tp_tag_extraction1.reset_index(drop=True),
                           df2.reset_index(drop=True))
        # Assert 2
        assert_frame_equal(tp_tag_extraction2.reset_index(drop=True),
                           df3.reset_index(drop=True))

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

        # Assume 1
        tn_tag_extraction1 = tag_extraction(df1, 'Name', 'A')
        # Assume 2
        tn_tag_extraction2 = tag_extraction(df1, 'Name', ['A', 'C'])

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

        # Assert
        with self.assertRaises(TypeError):
            tag_extraction(1.0, 'Name', 'A')

        with self.assertRaises(TypeError):
            tag_extraction(df1, 1, 'A')

        with self.assertRaises(TypeError):
            tag_extraction(df1, 'Name', {'A': 'a'})

        with self.assertRaises(TypeError):
            tag_extraction(df1, 'Name', all)

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

        # Assert
        with self.assertRaises(ValueError):
            tag_extraction(df1, 'names', 'A')

        with self.assertRaises(ValueError):
            tag_extraction(df1, 'A', 'Name')
