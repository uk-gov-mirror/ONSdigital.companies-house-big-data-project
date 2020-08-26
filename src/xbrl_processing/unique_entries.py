import pandas as pd


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
