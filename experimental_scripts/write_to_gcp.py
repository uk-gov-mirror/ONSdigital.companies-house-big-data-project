#function
def df_to_gbq(df,project_id,database,table,schema=None,key=None):
    '''
    Function to export a pandas dataframe to a big query table.
    
    Arguments:
    df (Dataframe)      = pandas dataframe
    project_id {string} = the GCP project id
    table {string}      = name of new Big Query table
    database {string}
    schema              = manual schema of dataframe 
    {list of dicts}       (default = None, optional)
    key (string)        = filepath of the authentication key (json) file
                        (default = None, optional)
    Returns:
        None
    '''
    import pandas

    table_id = dataset+'.'+table

    df.to_gbq(table_id,project_id=project_id,table_schema=schema,
              credentials=key)

    return None

#testing
import pandas as pandas
df = pandas.DataFrame(
    {
        'my_string': ['a', 'b', 'c'],
        'my_int64': [1, 2, 3],
        'my_float64': [4.0, 5.0, 6.0],
        'my_timestamp': [
            pandas.Timestamp("1998-09-04T16:03:14"),
            pandas.Timestamp("2010-09-13T12:03:45"),
            pandas.Timestamp("2015-10-02T16:00:00")
        ],
    }
)

df_to_gbq(df,'ons-companies-house-dev','test_db','test')


