import os
import numpy as np
import pandas as pd
import importlib
import time
import sys

# Custom import
from src.data_processing.xbrl_parser import XbrlParser

xbrl_parser = XbrlParser()

class XbrlExtraction:
    """
    """

    def __init__(self):
        self.__init__

    @staticmethod
    def get_filepaths(directory):

        """ Helper function -
        Get all of the filenames in a directory that
        end in htm* or xml.
        Under the assumption that all files within
        the folder are financial records. """

        files = [directory + "/" + filename
                    for filename in os.listdir(directory)
                        if (("htm" in filename.lower()) or ("xml" in filename.lower()))]

        month_and_year = ('').join(directory.split('-')[-1:])
        month, year = month_and_year[:-4], month_and_year[-4:]

        return files, month, year

    @staticmethod
    def progressBar(name, value, endvalue, bar_length = 50, width = 20):
            percent = float(value) / endvalue
            arrow = '-' * int(round(percent*bar_length) - 1) + '>'
            spaces = ' ' * (bar_length - len(arrow))
            sys.stdout.write(
                "\r{0: <{1}} : [{2}]{3}%   ({4} / {5})".format(
                    name,
                    width,
                    arrow + spaces,
                    int(round(percent*100)),
                    value,
                    endvalue
                )
            )
            sys.stdout.flush()
            if value == endvalue:
                sys.stdout.write('\n\n')

    @staticmethod
    def retrieve_list_of_tags(dataframe, column, output_folder):
        """
        Save dataframe containing all unique tags to txt format in specified directory.

        Arguements:
            dataframe:     tabular data
            column:        location of xbrl tags
            output_folder: user specified file location
        Returns:
            None
        Raises:
            None
        """
        list_of_tags = dataframe['name'].tolist()
        list_of_tags_unique = list(set(list_of_tags))

        print(
            "Number of tags in total: {}\nOf which are unique: {}".format(len(list_of_tags), len(list_of_tags_unique))
        )

        with open(output_folder + "/" + folder_year + "-" + folder_month + "_list_of_tags.txt", 'w') as f:
            for item in list_of_tags_unique:
                f.write("%s\n" % item)

    @staticmethod
    def get_tag_counts(dataframe, column, output_folder):
        """
        Save dataframe containing all unique tags to txt format in specified directory.

        Arguements:
            dataframe:     tabular data
            column:        location of xbrl tags
            output_folder: user specified file location
        Returns:
            None
        Raises:
            None
        """
        cache = dataframe
        cache["count"] = cache.groupby(by = column)[column].transform("count")
        cache.sort_values("count", inplace = True, ascending = False)
        cache.drop_duplicates(subset = [column, "count"], keep = "first", inplace = True)
        cache = cache[[column, "count"]]

        print(cache.shape)

        cache.to_csv(
            output_folder + "/" + folder_year + "-" + folder_month + "_unique_tag_frequencies.csv",
            header = None,
            index = True,
            sep = "\t",
            mode = "a"
        )

    @staticmethod
    def build_month_table(list_of_files):
        """
        """
        process_start = time.time()

        # Empty table awaiting results
        results = pd.DataFrame()

        COUNT = 0

        # For every file
        for file in list_of_files:

            COUNT += 1

            # Read the file
            doc = xbrl_parser.process_account(file)

            # tabulate the results
            doc_df = xbrl_parser.flatten_data(doc)

            # append to table
            results = results.append(doc_df)

            XbrlExtraction.progressBar("XBRL Accounts Parsed", COUNT, len(list_of_files), bar_length = 50, width = 20)

        print("Average time to process an XBRL file: \x1b[31m{:0f}\x1b[0m".format((time.time() - process_start) / 60, 2), "seconds")

        return results

    @staticmethod
    def output_xbrl_month(dataframe, output_folder, file_type = "csv"):
        """
        Save dataframe to csv format in specified directory, with particular naming scheme "YYYY-MM_xbrl_data.csv".

        Arguments:
            dataframe:     tabular data
            output_folder: user specified file destination
        Returns:
            None
        Raises:
            None
        """
        if file_type == "csv":
            dataframe.to_csv(
                output_folder
                    + "/"
                    + folder_year
                    + "-"
                    + folder_month
                    + "_xbrl_data.csv",
                index = False,
                header = True
            )
        else:
            print("I need a CSV for now...")
