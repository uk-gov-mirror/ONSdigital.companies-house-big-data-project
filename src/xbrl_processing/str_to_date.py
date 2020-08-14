def str_to_date(df, date_col, replace="y", col_name=None):
    #dependancies 
    import pandas as pd
    from datetime import datetime
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
    
    #if input column is string - convert
    if type(date_col) == str:
        x = pd.to_datetime(df[date_col], infer_datetime_format=True)
        if replace == "y":
            df[date_col] = x
        elif replace == "n":
            df[col_name] = x
        else:
            print('Error: replace needs to be a "y" or "n"')
    #otherwise print error
    else: 
        output = print("Error: date_col needs to be a string")
    return df