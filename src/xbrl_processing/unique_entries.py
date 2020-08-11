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

    unique = df.drop_duplicates(col_name)

    if out_list:
        x = unique[col_name].tolist()

    if not out_list:
        x = unique
        
    output = x
    return output
