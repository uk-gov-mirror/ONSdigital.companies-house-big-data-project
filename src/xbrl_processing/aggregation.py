import pandas as pd


def aggregation(df, groupby_cols, agg_method, agg_cols, naming = True):
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
        A dataframe that has been aggregated according to the specfied 
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
    if agg_method not in ['sum', 'mean', 'count', 'var', 'std', 'first', \
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