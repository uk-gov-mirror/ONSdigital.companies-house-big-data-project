import pandas as pd
import openpyxl


# Function to read in the combined csv for list of tags
def read_in_file(file):
    """
    Input:
        :param file: file

    Description:
        Function opens the specified csv file and the two columns 'tag_name' and 'frequency' so we canascertain how
        often the tag occurs within the dataset.

    Output:
        :return: DataFrame consisting of two columns - 'tag_name' and 'frequency'.
    """
    input_data = pd.read_csv(file, header=None, names=['tag_name', 'frequency'], delim_whitespace=True,
                             dtype={'frequency': int})
    return input_data


test = read_in_file('/home/kirstyc/test/2011_xbrl.csv')


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
           :return: An Excel workbook containing a worksheet for each DataFrame as specified.
       """

    if create_new_workbook:
        with pd.ExcelWriter(directory + filename + '.xlsx') as writer:
            output = input_data.to_excel(writer, sheet_name=sheet_name)
            #input_data.to_excel(writer, sheet_name=sheet_name)

    if not create_new_workbook: # appends a new worksheet to the workbook
        with pd.ExcelWriter(directory + filename + '.xlsx', mode='a') as writer:
            output = input_data.to_excel(writer, sheet_name=sheet_name)

    return output


test1 = write_to_excel(test, '/home/kirstyc/test/', 'cha_276_2011_list_of_tags',
                       '2011', create_new_workbook=True)

test2 = pd.read_excel('/home/kirstyc/test/cha_276_2011_list_of_tags.xlsx', index_col=0)
print(test2)

# Cannot view the xlsx in VM so reading the workbook 'test_cha_276.xlsx' into here to check it, one worksheet at a time
#test3 = pd.read_excel('/home/kirstyc/test/test_cha_276.xlsx', sheet_name=['data_2013'])

