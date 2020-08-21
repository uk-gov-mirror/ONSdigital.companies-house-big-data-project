import pandas as pd

# The test data to call in
data_2011 = pd.read_csv("/home/kirstyc/test/2011_xbrl.csv", nrows=200)
data_2012 = pd.read_csv("/home/kirstyc/test/2012_xbrl.csv", nrows=200)
data_2013 = pd.read_csv("/home/kirstyc/test/2013_xbrl.csv", nrows=200)


# for CHA_276 - get the tag names and frequencies for each year and create a spreadsheet with a worksheet/tab for each
# year.
def tags_and_frequencies(df, col_name):
    """
    Input:
        :param df: DataFrame
        :param col_name: name of the column you want the frequency of (string)

    Description:
        Function takes a DataFrame as an input and using the specified column (col_name), outputs the values within this
        column along with the frequency of their occurrence as a filtered DataFrame.

    Output:
        :return: DataFrame with the specified column and another column containing the count.
    """

    tags_and_count = df[col_name].value_counts()

    df_tags_and_counts = pd.DataFrame(tags_and_count).reset_index()
    df_tags_and_counts.columns = [col_name, 'frequency']

    return df_tags_and_counts

annual_2011 = tags_and_frequencies(data_2011, 'name')
annual_2012 = tags_and_frequencies(data_2012, 'name')
annual_2013 = tags_and_frequencies(data_2013, 'name')

# Writes to an Excel workbook
def write_to_excel(input_data, directory, filename, sheet_name, create_new_workbook = True):
    """
       Input:
           :param input_data: DataFrame (df)
           :param directory: filepath to where you want the file saved (str)
           :param filename: the name of the file to create without the .xlsx extension (str)
           :param sheet_name: name of the spreadsheet/tab (str)
           :param create_new_workbook: Create a new workbook (default = True) or write to an existing one (False) (bool)

       Description:
           Function takes a DataFrame as an input and writes it to a worksheet within an Excel workbook.
           create_new_workbook = True creates a workbook containing one worksheet of data and
           create_new_workbook = False appends a new worksheet to an existing workbook as specified.

       Output:
           :return: An Excel workbook containing a worksheet for each DataFrame.
       """

    if create_new_workbook:
        with pd.ExcelWriter(directory + filename + '.xlsx') as writer:
            output = input_data.to_excel(writer, sheet_name=sheet_name)
            #input_data.to_excel(writer, sheet_name=sheet_name)

    if not create_new_workbook: # appends a new worksheet to the workbook
        with pd.ExcelWriter(directory + filename + '.xlsx', mode='a') as writer:
            output = input_data.to_excel(writer, sheet_name='data_2013')

    return output


test = write_to_excel(annual_2013, '/home/kirstyc/test/', 'test_again2', '2013', create_new_workbook = True)

# Cannot view the xlsx in VM so reading the workbook 'test_cha_276.xlsx' into here to check it, one worksheet at a time
data2013 = pd.read_excel('/home/kirstyc/test/test_cha_276.xlsx', sheet_name=['data_2013'])


# NEXT STEP:

# Size of each month of data for each year.

# example - if all the files with 2010 < 20 GB, merge them into an annual file i.e. all months of 2010 in one
# .csv file ready to ne ingested into DAP.
# if all files with 2010 > 20 GB, if Jan, Feb and March 2010 < 20 GB, merge these files. If not, note the files and size
# to add in the JIRA ticket.

