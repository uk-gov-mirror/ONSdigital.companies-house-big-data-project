import pandas as pd
#display
pd.set_option('display.max_colwidth',None)
pd.set_option('max_columns',None)

#functions
def col_cast(df,col,col_type):
    '''
    Converts a pandas dataframe column or list of columns into
    a specified type.
    '''
    #can include all standard types
    allowed_types = ['str','datetime64[ns]','int64','int32','bool']

    #convert col to list if string
    if type(col) is str:
        col =[col]
    else:
        pass
    #loop over inputted columns and apply type
    for c in col:
        if col_type == 'date':
            df[c] = df[c].astype('datetime64[ns]').dt.date

        elif col_type in allowed_types:
            df[c] = df[c].astype(col_type)
        df = df
    return df


def parsed_csv_clean(import_path, export_path=''):
    """
    cleans a parsed xbrl csv file (from the xbrl_parsed_data storage)
    removing the index, removing unwanted string character from the value
    column and casting string and date(time) columns.

    inputs:
        import_path = the filepath to the parsed xbrl csv file
        export_path = the filepath to export the cleaned csv file

    returns:
        None
    """
    ## imports
    import pandas as pd
    import csv
    ## parameters
    unwanted_chars = ['  ', '"', '\n']
    str_cols = ['name', 'unit', 'value', 'doc_name', 'doc_type', 'arc_name', 'doc_companieshouseregisterednumber',
                'doc_standard_type', 'doc_standard_link',]
    date_cols = ['date', 'doc_balancesheetdate', 'doc_standard_date', 'doc_upload_date']

    wanted_cols = ['date','name','unit','value', 'doc_name', 'doc_type','doc_upload_date', 'arc_name','parsed','doc_balancesheetdate', 'doc_companieshouseregisterednumber','doc_standard_type', 'doc_standard_date', 'doc_standard_link',]

    ### turn all to string for now - redo when dates sorted (March 2015 example)###
    str_cols = str_cols + date_cols

    ## import data
    df = pd.read_csv(import_path, lineterminator="\n")

    # remove unwanted chars
    for char in unwanted_chars:
        df.value = df.value.str.replace(char, '')

    #limit to desired columns
    df = df[wanted_cols]

    # cast columns
    df = col_cast(df, str_cols, 'str')
    #df = col_cast(df, date_cols, 'datetime64[ns]')

    ##export
    if export_path == '':
        export_path = import_path
    # write to csv with no index, tab deliminated, and quoting all non numeric data
    print('Exporting cleaned dataframe to {}'.format(export_path))
    df.to_csv(export_path, index=False, sep="\t", line_terminator='\n', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

    return None
#parsed_csv_clean('/shares/xbrl_parsed_data/2020-April_xbrl_data.csv','/shares/test_parsed_data/2020-April_xbrl_data.csv')
import os
import glob
path = '/shares/xbrl_parsed_data/'
extension = '.csv'
os.chdir(path)
xbrl_files = os.listdir(path)
xbrl_files = [csv for csv in xbrl_files if csv.endswith(extension)]

for file in xbrl_files:
    print('Exporting {}......'.format(file))
    parsed_csv_clean('/shares/xbrl_parsed_data/'+file,'/shares/test_parsed_data/'+file)
    print('Successfully export {}!'.format(file))


#print(df.head())
#import_path = '/shares/test_xbrl_data/'
#df =pd.read_csv(import_path, lineterminator="\n")


