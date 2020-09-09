import os
import csv
import itertools
import pandas as pd
from typing import List, Tuple
from datetime import datetime

"""
# The test data to call in
data_2010 = pd.read_csv("/home/kirstyc/test/2011_xbrl.csv", nrows=200)
data_2011 = pd.read_csv("/home/kirstyc/test/2012_xbrl.csv", nrows=200)
#data_2013 = pd.read_csv("/home/kirstyc/test/2013_xbrl.csv", nrows=200)
"""

def combine_csv(files: List[str], outfile: str):
    '''Combines all csv files listed in files into a single cs file using csv.writer.
    Unsuitable if files do not have the same format. Faster than the pandas version.'''

    print("Processing " + str(len(files)) + " files...")
    with open(outfile, "w") as w:
        writer = csv.writer(w, delimiter=',')
        for n, file in enumerate(files):
            print("Processing " + file + "...")
            with open(file, "r") as f:
                reader = csv.reader(f, delimiter=',')
                if n > 0: next(reader)
                for row in reader:
                 writer.writerow(row)

def combine_csv_pd(files: List[str], outfile: str):
    '''Combines all csv files listed in files into a single csv file using pandas'''

    combined_csv = pd.concat([pd.read_csv(f, engine = 'python') for f in files])
    combined_csv.to_csv(outfile, index = False)

def _add_path(indir: str, files: str):
    '''returns list with paths appended to file names'''
    return [indir+i for i in files]

def merge_files_by_year(indir: str, outdir: str, year: int, quarter=None):
    ''' Finds all csv files with directory indir, groups files into
    individual years and calls combine_csv function to merge into
    single file given that the years falls within the range specified
    by the user.'''

    # If the input directory is valid...
    if os.path.exists(indir):

        # Create the output folder if it doesn't exist
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        # Get all csv files which contain the year specified in their filenames
        files = os.listdir(indir)
        files = ([i for i in files if i[-4:] == '.txt'
                  and i[:4].isnumeric() and int(i[:4]) == year])

        # Sort all files by the month contained in the file name
        files = sorted(files, key=lambda day: datetime.strptime(day.split("-")[1].split("_")[0], "%B"))

        # Create a list of months based on what quarter in the year has been specified
        if quarter == 1:
            quarter_list = ['January', 'February', 'March']
        elif quarter == 2:
            quarter_list = ['April', 'May', 'June']
        elif quarter == 3:
            quarter_list = ['July', 'August', 'September']
        elif quarter == 4:
            quarter_list = ['October', 'November', 'December']
        else:
            quarter_list = []

        # Filter out those files not in the specified quarter,
        # if a quarter has been specified
        if quarter != None:
            files = [f for f in files if f.split("-")[1].split("_")[0] in quarter_list]
            quarter_str = "q" + str(quarter)
        else:
            quarter_str = ""

        # Prepend input directory to filenames
        files = [indir + f for f in files]

        # Combine the csvs
        combine_csv(files, outdir + str(year) + quarter_str + 'annual_list_of_tags_xbrl.csv')

    else:
        print("Input file path does not exist")

if __name__ == '__main__':

    indir = '/shares/data/20200519_companies_house_accounts/logs/'
    outdir = '/home/kirstyc/test0909/'
    year = 2011
    quarter = None

    merge_files_by_year(indir, outdir, year, quarter)

import pandas as pd
test = pd.read_csv('/home/kirstyc/test0909/2011_xbrl.csv', header=None, names=['tag_name'])



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

annual_2011 = tags_and_frequencies(test, 'tag_name')


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


test = write_to_excel(annual_2011, '/home/kirstyc/repos/companies_house_accounts/data/', 'cha_276_2011_list_of_tags', '2011', create_new_workbook = True)

test2 = pd.read_excel('/home/kirstyc/test0909/2011_list_of_tags.xlsx', index_col=0)
print(test2)

# Cannot view the xlsx in VM so reading the workbook 'test_cha_276.xlsx' into here to check it, one worksheet at a time
#data2013 = pd.read_excel('/home/kirstyc/test/test_cha_276.xlsx', sheet_name=['data_2013'])


# NEXT STEP:

# Size of each month of data for each year.

# example - if all the files with 2010 < 20 GB, merge them into an annual file i.e. all months of 2010 in one
# .csv file ready to ne ingested into DAP.
# if all files with 2010 > 20 GB, if Jan, Feb and March 2010 < 20 GB, merge these files. If not, note the files and size
# to add in the JIRA ticket.

