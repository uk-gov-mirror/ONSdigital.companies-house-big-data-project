import os
import csv
import itertools
import pandas as pd
from typing import List
from datetime import datetime


class XbrlCsvAppender:
    """ This class combines the csvs. """

    def __init__(self):
        self.__init__

    @staticmethod
    def combine_csv(files: List[str], outfile: str):
        """Combines all csv files listed in files into a single csv file using
        csv.writer. Unsuitable if files do not have the same format. Faster
        than the pandas version.

        Arguments:
            files:      List of csv files specified by user
            outfile:    Filepath to write combined csv file
        Returns:
            None
        Raises:
            None
        """

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

    @staticmethod
    def combine_csv_pd(files: List[str], outfile: str):
        """Combines all csv files listed in files into a single csv file using
        pandas
        """

        combined_csv = pd.concat([pd.read_csv(f, engine='python')
                                  for f in files])
        combined_csv.to_csv(outfile, index=False)

    @staticmethod
    def _add_path(indir: str, files: str):
        """returns list with paths appended to file names"""
        return [indir+i for i in files]

    @staticmethod
    def merge_files_by_year(indir: str, outdir: str, year: str, quarter=""):
        """ Finds all csv files with directory indir, groups files into
        individual years and calls combine_csv function to merge into
        single file given that the years falls within the range specified
        by the user.

        Arguments:
            indir:      Path of directory to search for csv files in
            outdir:     Path of directory to write resulting files
            year:       Find all files from this year
            quarter:    Find all files from this quarter
        Returns:
            None
        Raises:
            None
        """

        year = int(year)
        if quarter == "None":
            quarter = None
        else:
            quarter = int(quarter)

        # If the input directory is valid...
        if os.path.exists(indir):

            # Create the output folder if it doesn't exist
            if not os.path.exists(outdir):
                os.mkdir(outdir)

            # Get all csv files which contain the year specified in their
            # filenames
            files = os.listdir(indir)
            files = ([i for i in files if i[-4:] == '.csv'
                      and i[:4].isnumeric() and int(i[:4]) == year])

            # Sort all files by the month contained in the file name
            files = sorted(files,
                           key=lambda day: datetime.strptime(
                               day.split("-")[1].split("_")[0], "%B"))

            # Create a list of months based on what quarter in the year has
            # been specified
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
                files = [f for f in files if f.split("-")[1].split("_")[0]
                         in quarter_list]
                quarter_str = "q" + str(quarter)
            else:
                quarter_str = ""

            # Prepend input directory to filenames
            files = [indir + f for f in files]

            # Combine the csvs
            XbrlCsvAppender.combine_csv(files, outdir + str(year)
                                            + quarter_str + '_xbrl.csv')
        else:
            print("Input file path does not exist")

if __name__ == '__main__':

    indir = '/shares/data/20200519_companies_house_accounts/xbrl_parsed_data/'
    outdir = '/home/peterd/test/'
    year = 2011
    quarter = None

    XbrlCsvAppender.merge_files_by_year(indir, outdir, year, quarter)
