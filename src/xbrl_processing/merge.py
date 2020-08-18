def merge(df1, df2, df1_pk, df2_pk, join_method = "left", pk_keep = "df1"):
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
                - the pk_keep is diffrent from 'df1', 'df2', 'both'

    Output:
        Merged dataframe joined using the chosen method and name convention.
    """
    
    # Check that both input df are actually dateframe type.
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
         raise TypeError("The first two arguments (df1, df2) need to be dataframes")
    
    # Check the primary keys are strings.
    if not isinstance(df1_pk, str) or not isinstance(df2_pk, str):
        raise TypeError("The primary keys (df1_pk, df2_pk) you want to use from the two dataframes should be strings")
    
    # Check the join_method passed is a valid one.
    if join_method not in ['left', 'right', 'outer', 'inner']:
        raise ValueError("The join_method should be one of the following: left, right, outer or inner")
    
    # Check the pk_keep is one of 'df1', 'df2', 'both' options
    if pk_keep not in ['df1', 'df2', 'both']:
        raise ValueError( "The pk_keep should be one of the following: df1, df2, both")
    
    # Standardise the primary key, depending on pk_keep input.
    if df1_pk != df2_pk:
        if pk_keep == "df1":
            df2 = df2.rename(columns={df2_pk:df1_pk})
        elif pk_keep == "df2":
            df1 = df1.rename(columns={df1_pk:df2_pk})
        else:
            pass
        
    # Merge df's depending on the pk_keep input.
    if pk_keep == "df1":
        merge_df = df1.merge(df2, how = join_method, on = df1_pk)
    if pk_keep == "df2":
        merge_df = df1.merge(df2, how = join_method, on = df2_pk)
    if pk_keep == "both":
        merge_df = df1.merge(df2, how = join_method, left_on = df1_pk, right_on = df2_pk)

    return merge_df