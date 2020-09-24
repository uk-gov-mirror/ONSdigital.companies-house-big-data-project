import os
import numpy as np
import pandas as pd
import importlib
import time
import sys

# Custom import
from src.data_processing.xbrl_parser import XbrlParser

xbrl_parser = XbrlParser()

class XbrlExtraction:
    """
    """

    def __init__(self):
        self.__init__

    @staticmethod
    def get_filepaths(directory):

        """ Helper function -
        Get all of the filenames in a directory that
        end in htm* or xml.
        Under the assumption that all files within
        the folder are financial records. """

        files = [directory + "/" + filename
                    for filename in os.listdir(directory)
                        if (("htm" in filename.lower()) or ("xml" in filename.lower()))]

        month_and_year = ('').join(directory.split('-')[-1:])
        month, year = month_and_year[:-4], month_and_year[-4:]

        return files, month, year

    @staticmethod
    def progressBar(name, value, endvalue, bar_length = 50, width = 20):
            percent = float(value) / endvalue
            arrow = '-' * int(round(percent*bar_length) - 1) + '>'
            spaces = ' ' * (bar_length - len(arrow))
            sys.stdout.write(
                "\r{0: <{1}} : [{2}]{3}%   ({4} / {5})".format(
                    name,
                    width,
                    arrow + spaces,
                    int(round(percent*100)),
                    value,
                    endvalue
                )
            )
            sys.stdout.flush()
            if value == endvalue:
                sys.stdout.write('\n\n')

    @staticmethod
    def retrieve_list_of_tags(dataframe, column, output_folder, folder_month, folder_year):
        """
        Save dataframe containing all unique tags to txt format in specified directory.

        Arguements:
            dataframe:     tabular data
            column:        location of xbrl tags
            output_folder: user specified file location
        Returns:
            None
        Raises:
            None
        """
        list_of_tags = dataframe['name'].tolist()
        list_of_tags_unique = list(set(list_of_tags))

        print(
            "Number of tags in total: {}\nOf which are unique: {}".format(len(list_of_tags), len(list_of_tags_unique))
        )

        with open(output_folder + "/" + folder_year + "-" + folder_month + "_list_of_tags.txt", 'w') as f:
            for item in list_of_tags_unique:
                f.write("%s\n" % item)

    @staticmethod
    def get_tag_counts(dataframe, column, output_folder, folder_month, folder_year):
        """
        Save dataframe containing all unique tags to txt format in specified directory.

        Arguements:
            dataframe:     tabular data
            column:        location of xbrl tags
            output_folder: user specified file location
        Returns:
            None
        Raises:
            None
        """

        # Check that the input df is actually dataframe type.
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("The first argument (df) needs to be a dataframe")

        #if not isinstance(groupby_cols, list) or not isinstance(agg_cols, list):
        #    raise TypeError("The groupby_cols and agg_cols need to be a list")

        if column not in list(dataframe.columns):
            raise ValueError("The column should exist in the dataframe passed")

        cache = dataframe
        cache["count"] = cache.groupby(by = column)[column].transform("count")
        cache.sort_values("count", inplace = True, ascending = False)
        cache.drop_duplicates(subset = [column, "count"], keep = "first", inplace = True)
        cache = cache[[column, "count"]]

        print(cache.shape)

        cache.to_csv(
            output_folder + "/" + folder_year + "-" + folder_month + "_unique_tag_frequencies.csv",
            header = None,
            index = True,
            sep = "\t",
            mode = "a"
        )

    @staticmethod
    def build_month_table(list_of_files):
        """
        """
        process_start = time.time()

        # Empty table awaiting results
        results = pd.DataFrame()

        COUNT = 0

        # For every file
        for file in list_of_files:

            COUNT += 1

            # Read the file
            doc = xbrl_parser.process_account(file)

            # tabulate the results
            doc_df = xbrl_parser.flatten_data(doc)

            # append to table
            results = results.append(doc_df)

            XbrlExtraction.progressBar("XBRL Accounts Parsed", COUNT, len(list_of_files), bar_length = 50, width = 20)

        print("Average time to process an XBRL file: \x1b[31m{:0f}\x1b[0m".format((time.time() - process_start) / 60, 2), "minutes")

        return results

    @staticmethod
    def output_xbrl_month(dataframe, output_folder, folder_month, folder_year, file_type = "csv"):
        """
        Save dataframe to csv format in specified directory, with particular naming scheme "YYYY-MM_xbrl_data.csv".

        Arguments:
            dataframe:     tabular data
            output_folder: user specified file destination
        Returns:
            None
        Raises:
            None
        """
        if file_type == "csv":
            dataframe.to_csv(
                output_folder
                    + "/"
                    + folder_year
                    + "-"
                    + folder_month
                    + "_xbrl_data.csv",
                index = False,
                header = True
            )
        else:
            print("I need a CSV for now...")


class XbrlSubsets:
    """
    """

    def __init__(self):
        self.__init__

    @staticmethod
    def aggregation(df, groupby_cols, agg_method, agg_cols, naming=True):
        """
        Inputs:
            df = dataframe you would like to use (dataframe)
            groupby_cols = list of column names you would like to use as
                           your groupby clause (list)
            agg_method = name of the aggregation method you would like
                         to use (string)
                         * 'sum', 'mean', 'count', 'var', 'std', 'first',
                           'last', 'min' or 'max'
            agg_cols = columns you would like to perform the aggregation
                       method on (list)
            naming = naming convention for the output columns (Boolean)
                     * True (default) = keep column names same as input df
                     * False = before the column name add the method of
                               aggregation (agg_method) separated by space

        Description:
            Function that allows for a dataframe to be aggregated by a
            specified method, returning a reduced dataframe containing the
            groupedby columns and the aggregation columns.

        Output:
            A dataframe that has been aggregated according to the specified
            method using the specified groupby and aggregation columns.
        """

        # Check that the input df is actually dataframe type.
        if not isinstance(df, pd.DataFrame):
            raise TypeError("The first argument (df) needs to be a dataframe")

        # Turn the passed groupby_cols into list if only one column, i.e. string.
        if isinstance(groupby_cols, str):
            groupby_cols = [groupby_cols]

        # Turn the passed agg_cols into list if only one column, i.e. string.
        if isinstance(agg_cols, str):
            agg_cols = [agg_cols]

            # Raise an error if the arguments groupby_cols and agg_cols are not a list.
        if not isinstance(groupby_cols, list) or not isinstance(agg_cols, list):
            raise TypeError("The groupby_cols and agg_cols need to be a list")

        # Raise an error if not all of the columns in groupby_cols
        # and agg_cols are in the original input dataframe.
        if (not all(columns in df.columns for columns in groupby_cols) or
                not all(columns in df.columns for columns in agg_cols)):
            raise TypeError("The selected columns are not in the input dataframe")

        # Check agg_method is a valid one. If yes, do the groupby and aggregation.
        if agg_method not in ['sum', 'mean', 'count', 'var', 'std', 'first',
                              'last', 'min', 'max']:
            raise ValueError(("The agg_method should be: sum, mean, count, var, "
                              "std, first, last, min or max"))
        else:
            output = df.groupby(groupby_cols).agg(agg_method)
            agg_cols.extend(groupby_cols)
            output = output[output.columns[output.columns.isin(agg_cols)]]

        # Naming the columns depending on the naming value passed.
        if naming == True:
            pass
        elif naming == False:
            output = output.rename(columns=lambda s: agg_method + '  ' + s)
        else:
            raise TypeError("The naming argument needs to be Boolean")

        return output

    @staticmethod
    def merge(df1, df2, df1_pk, df2_pk, join_method="left", pk_keep="df1"):
        """
        Inputs:
            df1: Dataframe 1 (left dataframe)
            df2: Dataframe 2 (right dataframe)
            df1_pk: Name of the primary key for df1 (string)
            df2_pk: Name of the primary key for df2 (string)
            join_method: type of join method to be performed (conditional string);
                         left, right, outer, inner --- "left" is the default
            pk_keep: dataframe primary key you want to retain (conditional string);
                     "df1": retain naming convention for df1_pk (the default)
                     "df2": retain naming convention for df2_pk
                     "both": retain the original name of the pk's

        Description:
            Function to join the two dataframes together by a specified method and name convention.
            Allows users to choose the join method, the primary keys and which primary key to retain (might be both).
            Throws errors if:
                    - the df1 and df2 are not a dataframe type
                    - the df1_pk and df2_pk are not a string
                    - the join_method is not a valid type of merge to be performed
                    - the pk_keep is different from 'df1', 'df2', 'both'

        Output:
            Merged dataframe joined using the chosen method and name convention.
        """

        # Check that both input df are actually dataframe type.
        if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
            raise TypeError("The first two arguments (df1, df2) need to be dataframes")

        # Check the primary keys are strings.
        if not isinstance(df1_pk, str) or not isinstance(df2_pk, str):
            raise TypeError(
                "The primary keys (df1_pk, df2_pk) you want to use from the two dataframes should be strings")

        # Check the join_method passed is a valid one.
        if join_method not in ['left', 'right', 'outer', 'inner']:
            raise ValueError("The join_method should be one of the following: left, right, outer or inner")

        # Check the pk_keep is one of 'df1', 'df2', 'both' options
        if pk_keep not in ['df1', 'df2', 'both']:
            raise ValueError("The pk_keep should be one of the following: df1, df2, both")

        # Standardise the primary key, depending on pk_keep input.
        if df1_pk != df2_pk:
            if pk_keep == "df1":
                df2 = df2.rename(columns={df2_pk: df1_pk})
            elif pk_keep == "df2":
                df1 = df1.rename(columns={df1_pk: df2_pk})
            else:
                pass

        # Merge dfs depending on the pk_keep input.
        if pk_keep == "df1":
            merge_df = df1.merge(df2, how=join_method, on=df1_pk)
        if pk_keep == "df2":
            merge_df = df1.merge(df2, how=join_method, on=df2_pk)
        if pk_keep == "both":
            merge_df = df1.merge(df2, how=join_method, left_on=df1_pk, right_on=df2_pk)

        return merge_df

    @staticmethod
    def tag_extraction(df, tag_col, wanted_tag):
        """
        Inputs:
            :param df: DataFrame
            :param tag_col: name of column that contains tag name (string)
            :param wanted_tag: name of extracted tag(s) (string or list)

        Description:
            Function filters DataFrame to extract xbrl tags, returning the filtered DataFrame.
            Throws error if the wanted tags are not a string or list

        Output:
            :return: DataFrame filtered to contain only the required tag(s)
        """

        if not isinstance(df, pd.DataFrame):
            raise TypeError("The first argument (df) needs to be a dataframe")

        if not isinstance(tag_col, str):
            raise TypeError("The tag_col needs to be a string")

        if tag_col not in list(df.columns):
            raise ValueError("The tag_col should exist in the dataframe passed")

        if isinstance(wanted_tag, str):
            tag = [wanted_tag]
        elif isinstance(wanted_tag, list):
            tag = wanted_tag
        else:
            raise TypeError("The wanted_tag needs to be a string or list")

        output = df[df[tag_col].isin(tag)]

        return output

    @staticmethod
    def unique_entries(df, col_name, out_list=True):
        """
        Input:
            :param df: DataFrame
            :param col_name: name of column you want to find unique values of (string)
            :param out_list: option to choose output as a list or DataFrame (True or False respectively)

        Description:
            Function takes a DataFrame as input and selects the specified column, outputting
            the unique values present within that column, as either a list or filtered DataFrame

        Output:
            :return: List or DataFrame column of unique values
        """

        if not isinstance(df, pd.DataFrame):
            raise TypeError("The first argument (df) needs to be a dataframe")

        if not isinstance(col_name, str):
            raise TypeError("The col_name needs to be a string")

        if col_name not in list(df.columns):
            raise ValueError("The col_name should exist in the dataframe you pass")

        unique = df.drop_duplicates(col_name)

        if not isinstance(out_list, bool):
            raise TypeError("The out_list argument needs to be a Boolean")

        if out_list:
            x = unique[col_name].tolist()

        if not out_list:
            x = unique

        output = x

        return output

    @staticmethod
    def str_to_date(df, date_col, replace="y", col_name=None):
        """
        Inputs:
            :param df: DataFrame
            :param date_col: name of date column (string)
            :param replace: option for new date column to replace original column, "y" or "n" respectively (string, "y" or "n")
            :param col_name: if replace = "n", name of new date column (string)

        Description:
            Converts a string column into a datetime format (of the formatting: yyyy-mm-dd)

        Output:
            :return: DataFrame by either replacing original column with date format or creating a new column of date format
        """

        if not isinstance(df, pd.DataFrame):
            raise TypeError("The first argument (df) needs to be a dataframe")

        if not isinstance(date_col, str):
            raise TypeError("The date_col needs to be a string")

        if replace not in ['y','n']:
            raise ValueError("The replace argument needs to 'y' or 'n'")

        if date_col not in list(df.columns):
            raise ValueError("The date_col needs to be a column present in the dataframe passed (df)")

        # if input column is string - convert
        if type(date_col) == str:
            x = pd.to_datetime(df[date_col], infer_datetime_format=True)
            if replace == "y":
                df[date_col] = x
            elif replace == "n":
                df[col_name] = x
            else:
                print('Error: replace needs to be a "y" or "n"')
        # otherwise print error
        else:
            print("Error: date_col needs to be a string")

        return df



