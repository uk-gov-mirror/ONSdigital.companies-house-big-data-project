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
        df2_datetime_inplace = pd.DataFrame([['A', '01/01/2001'],
                                            ['B', '02/02/2002'],
                                            ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
        df2_datetime_inplace['Date'] = pd.to_datetime(df2_datetime_inplace['Date'])

        df3_datetime_new_col = pd.DataFrame([['A', '01/01/2001'],
                                            ['B', '02/02/2002'],
                                            ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
        df3_datetime_new_col['New Date Col'] = pd.to_datetime(df3_datetime_new_col['Date'])

        # Assume
        subsets = XbrlSubsets()

        # Assume 1 - replace type of Date column with datetime instead of string.
        tp_str_to_date = subsets.str_to_date(df1_str_date, 'Date', replace="y")

        # Assume 2 - create new Date column called 'New Date Col' of type datetime.
        tp_str_to_date_2 = subsets.str_to_date(df1_str_date, 'Date', replace="n", col_name="New Date Col")

        # Assert 1
        assert_frame_equal(tp_str_to_date, df2_datetime_inplace)

        # Assert 2
        assert_frame_equal(tp_str_to_date_2, df3_datetime_new_col)


    def test_str_to_date_neg(self):
        """
        Negative test case for the str_to_date function.
        """

        df1_str_date = pd.DataFrame([['A', '01/01/2001'],
                                     ['B', '02/02/2002'],
                                     ['C', '03/03/2003']],
                                    columns=['Name', 'Date'])
        df2_datetime_inplace = pd.DataFrame([['A', '01/01/2001'],
                                             ['B', '02/02/2002'],
                                             ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
        df2_datetime_inplace['Date'] = pd.to_datetime(df2_datetime_inplace['Date'])

        df3_datetime_new_col = pd.DataFrame([['A', '01/01/2001'],
                                             ['B', '02/02/2002'],
                                             ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
        df3_datetime_new_col['New Date Col'] = pd.to_datetime(df3_datetime_new_col['Date'])


        # Assume
        subsets = XbrlSubsets()

        # Assume 1 - replace type of Date column with datetime instead of string.
        tn_str_to_date = subsets.str_to_date(df1_str_date, 'Date', replace="y", col_name=None)

        # Assume 2 - create new Date column called 'New Date Col' of type datetime.
        tn_str_to_date_2 = subsets.str_to_date(df1_str_date, 'Date', replace="n", col_name="New Date Col")

        # Assert 1
        self.assertEqual(tn_str_to_date.equals(df2_datetime_inplace), False)

        # Assert 2
        self.assertEqual(tn_str_to_date_2.equals(df3_datetime_new_col), False)

"""
    def test_values(self):
        
        Test case for the recall function.
        
        # Assume
        metrics = XbrlSubsets()

        # Assert
        with self.assertRaises(ValueError):
            metrics.str_to_date(None, 2)

        with self.assertRaises(ValueError):
            metrics.str_to_date(1, None)



import pandas as pd
df2_datetime_inplace = pd.DataFrame([['A', '01/01/2001'],
                                            ['B', '02/02/2002'],
                                            ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
df2_datetime_inplace['Date'] = pd.to_datetime(df2_datetime_inplace['Date'])
print(df2_datetime_inplace)
"""

import pandas as pd
df2_datetime_inplace = pd.DataFrame([['A', '01/01/2001'],
                                            ['B', '02/02/2002'],
                                            ['C', '03/03/2003']],
                                            columns=['Name', 'Date'])
df2_datetime_inplace['Date'] = pd.to_datetime(df2_datetime_inplace['Date'])
