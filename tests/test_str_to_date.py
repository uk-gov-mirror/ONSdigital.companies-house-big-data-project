import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

# Custom import
from src.data_processing.xbrl_pd_methods import XbrlSubsets


class StrToDate(unittest.TestCase):
    """

    """
    def test_str_to_date_pos(self):
        """
        Positive test case for the str_to_date function.
        """

        df1_str_date = pd.DataFrame([['A', '01/01/2001'],
                                    ['B', '02/02/2002'],
                                    ['C', '03/03/2003']],
                                    columns=['Name', 'Date'])
        df2_str_date = pd.DataFrame([['A', '01/01/2001'],
                                     ['B', '02/02/2002'],
                                     ['C', '03/03/2003']],
                                    columns=['Name', 'Date'])
        df3_datetime_inplace = pd.DataFrame([['A', '01/01/2001'],
                                            ['B', '02/02/2002'],
                                            ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
        df3_datetime_inplace['Date'] = pd.to_datetime(df3_datetime_inplace['Date'])

        df4_datetime_new_col = pd.DataFrame([['A', '01/01/2001'],
                                            ['B', '02/02/2002'],
                                            ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
        df4_datetime_new_col['New Date Col'] = pd.to_datetime(df4_datetime_new_col['Date'])

        # Assume
        subsets = XbrlSubsets()

        # Assume 1 - replace type of Date column with datetime instead of string.
        tp_str_to_date = subsets.str_to_date(df1_str_date, 'Date', replace="y")

        # Assume 2 - create new Date column called 'New Date Col' of type datetime.
        tp_str_to_date_2 = subsets.str_to_date(df2_str_date, 'Date', replace="n", col_name="New Date Col")

        # Assert 1
        assert_frame_equal(tp_str_to_date.reset_index(drop=True), df3_datetime_inplace.reset_index(drop=True))

        # Assert 2
        assert_frame_equal(tp_str_to_date_2.reset_index(drop=True), df4_datetime_new_col.reset_index(drop=True))

    def test_str_to_date_neg(self):
        """
        Negative test case for the str_to_date function.
        """

        df1_str_date = pd.DataFrame([['A', '01/01/2001'],
                                     ['B', '02/02/2002'],
                                     ['C', '03/03/2003']],
                                    columns=['Name', 'Date'])
        df2_str_date = pd.DataFrame([['A', '01/01/2001'],
                                     ['B', '02/02/2002'],
                                     ['C', '03/03/2003']],
                                    columns=['Name', 'Date'])
        df3_datetime_inplace = pd.DataFrame([['A', '01/01/2001'],
                                             ['B', '02/02/2002'],
                                             ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
        df3_datetime_inplace['Date'] = pd.to_datetime(df3_datetime_inplace['Date'])

        df4_datetime_new_col = pd.DataFrame([['A', '01/01/2001'],
                                             ['B', '02/02/2002'],
                                             ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
        df4_datetime_new_col['New Date Col'] = pd.to_datetime(df4_datetime_new_col['Date'])

        # Assume
        subsets = XbrlSubsets()

        # Assume 1 - replace type of Date column with datetime instead of string.
        tn_str_to_date = subsets.str_to_date(df1_str_date, 'Date', replace="y")

        # Assume 2 - create new Date column called 'New Date Col' of type datetime.
        tn_str_to_date_2 = subsets.str_to_date(df2_str_date, 'Date', replace="n", col_name="New Date Col")

        # Assert 1
        self.assertEqual(tn_str_to_date.equals(df4_datetime_new_col), False)

        # Assert 2
        self.assertEqual(tn_str_to_date_2.equals(df3_datetime_inplace), False)

    def test_types(self):
        """
        Test types for the str_to_date function.
        """
        # Assume
        df1_str_date = pd.DataFrame([['A', '01/01/2001'],
                                    ['B', '02/02/2002'],
                                    ['C', '03/03/2003']],
                                    columns=['Name', 'Date'])

        # Assume
        subsets = XbrlSubsets()

        # Assert
        with self.assertRaises(TypeError):
            subsets.str_to_date(1.0, 'Date')

        with self.assertRaises(TypeError):
            subsets.str_to_date(df1_str_date, 5)

        with self.assertRaises(TypeError):
            subsets.str_to_date(df1_str_date, ['Name', 'Date'])

    def test_values(self):
        """
        Test values for the str_to_date function.
        """
        # Assume
        df1_str_date = pd.DataFrame([['A', '01/01/2001'],
                                     ['B', '02/02/2002'],
                                     ['C', '03/03/2003']],
                                    columns=['Name', 'Date'])

        # Assume
        subsets = XbrlSubsets()

        # Check error raised if string not present in column names
        with self.assertRaises(ValueError):
            subsets.str_to_date(df1_str_date, 'Yellow')

        #Check erorr if user combines two column names into one string
        with self.assertRaises(ValueError):
            subsets.str_to_date(df1_str_date, 'Name, Date')

        #Check error is raised if replace is not 'y' or 'n'
        with self.assertRaises(ValueError):
            subsets.str_to_date(df1_str_date, 'Name',replace='q')