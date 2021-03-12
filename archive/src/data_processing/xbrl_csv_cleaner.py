import pandas as pd
import csv
import os

class XbrlCSVCleaner:
    """
    Class to facilitate the cleaning of csv files outputted from the
    xbrl_parsing module.
    """

    def __init__(self):
        self.__init__

    @staticmethod
    def col_cast(df, col, col_type):
        """
        Converts a pandas dataframe column or list of columns into
        a specified type.

        Arguments:
            df:         A pandas dataframe
            col:        A list of columns to be converted from the dataframe
            col_type:   The type to convert the column to (from type specified
                        in allowed_types)
        Returns:
            df: The dataframe after the conversion has been applied
        Raises:
            None
        """

        # can include all standard types
        allowed_types = ['str', 'datetime64[ns]', 'int64', 'int32', 'bool']

        # convert col to list if string
        if type(col) is str:
            col = [col]
        else:
            pass
        # loop over inputted columns and apply type
        for c in col:
            if col_type == 'date':
                df[c] = df[c].astype('datetime64[ns]').dt.date

            elif col_type in allowed_types:
                df[c] = df[c].astype(col_type)
            df = df
        return df

if __name__ == "__main__":
    import gcsfs
    pd.set_option('display.max_colwidth', None)
    pd.set_option('max_columns', None)

    in_path = "gs://ons-companies-house-dev-test-parsed-csv-data/v2_parsed_data/2015-September_xbrl_data.csv"
    out_path = "gs://ons-companies-house-dev-test-parsed-csv-data/v2_parsed_data/2015-September_xbrl_data_cleaned.csv"

    cleaner = XbrlCSVCleaner()

    df = cleaner.parsed_csv_clean(in_path, export_path=out_path)