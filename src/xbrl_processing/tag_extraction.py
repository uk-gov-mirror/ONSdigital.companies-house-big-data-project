import pandas as pd

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
