import pandas as pd
import numpy as np

# read February 2010 data
data_test = pd.read_csv("/shares/data/20200519_companies_house_accounts/xbrl_parsed_data/2010-February_xbrl_data.csv",
                        lineterminator='\n',
                        low_memory=False)

# full dataset number of rows and columns
print(data_test.shape)

# see all columns
pd.set_option('display.max_columns', None)
# first 5 rows of the full dataset
print(data_test.head())

# only the columns we need
data_test_using = data_test[['doc_name', 'name', 'value']]
# see the first 5 rows
print(data_test_using.head())
#
# use only 100 rows to try the unstacking
data_try = data_test_using.head(100)

# getting error of same names for the columns so set a loop
# if two columns have the same name, append a number to the
# second one so they are both unique
for i in range(0, (len(data_try['name'])-1)):
    j = 1
    while j+i <= 99:
        if data_try.at[i, 'name'] == data_try.at[i + j, 'name']:
            data_try.at[i+j, 'name'] = data_try.at[i+j, 'name'] + str(j)
        j = j+1


# put the dataset in the format we want - setting indexes
data_try = data_try.set_index(['doc_name', 'name'])
# Do the unstacking
trying2 = data_try.unstack()
# See the columns
trying2.columns

trying2.to_csv('/home/ilinay/repos/companies-house-big-data-project/data/for_testing/melt_to_pivot.csv',)
trying2
# Number of rows and columns
print(trying2.shape)

# see the dataframe
trying2

trying2.columns.values
