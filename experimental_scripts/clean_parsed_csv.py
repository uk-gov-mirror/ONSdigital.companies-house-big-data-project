import pandas as pd
import csv
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

#parameters
unwanted_chars = ['  ','"','\n']
str_cols = ['name','unit','value','doc_name','doc_type','arc_name','doc_companieshouseregisterednumber','doc_standard_type','doc_standard_link']
date_cols = ['date','doc_balancesheetdate','doc_standard_date','doc_upload_date']

#inport data
df = pd.read_csv('/shares/test_parsed_data/2010-April_xbrl_data_quoted.csv', sep='\t', lineterminator="\n")
#delete index
df = df.drop(['Unnamed: 0'],axis=1)

#remove unwanted chars
for char in unwanted_chars:
    df.value = df.value.str.replace(char,'')
#cast columns
df = col_cast(df,str_cols,'str')
df = col_cast(df,date_cols,'datetime64[ns]')

#write to csv with no index, tab deliminated, and quoting all non numeric data
df.to_csv('/shares/test_parsed_data/2010-April_xbrl_data_quoted.csv',index=False,sep="\t",line_terminator='\n',quotechar='"',quoting=csv.QUOTE_NONNUMERIC)


