import os
import pandas as pd
import time
import csv

import sys
import gcsfs

# Custom import
# from src.data_processing.xbrl_parser import XbrlParser
# xbrl_parser = XbrlParser()

class XbrlExtraction:
    """ This is a class for extracting the XBRL data. """

    def __init__(self, fs):
        self.__init__
        self.fs = fs

    def get_filepaths(self, directory):
        """
        Helper function -
        Get all of the filenames in a directory that end in htm* or xml (under
        the assumption that all files within the folder are financial records).
        Arguments:
            directory: User specified directory (str)
        Returns:
            files:  List of all paths of xml or html files in directory (list)
            month:  The month of the specified directories accounts (str)
            year:   The year of the specified directories accounts (str)
        Raises:
            TypeError: If the path of the directory is not a string
        """
        # Check argument is of correct type
        if not isinstance(directory, str):
            raise TypeError("The input argument 'directory' \
            needs to be a string")

        files = [filename for filename in self.fs.ls(directory)
                        if ((".htm" in filename.lower())
                            or (".xml" in filename.lower()))
                 ]

        month_and_year = ''.join(directory.split('-')[-1:])
        month, year = month_and_year[:-4], month_and_year[-4:]

        return files, month, year

    @staticmethod
    def progressBar(name, value, endvalue, rows, batches,memory,uploading=False,
                    bar_length=50, width=20):
        """
        Function that can be called upon if a progress bar needs to be
        displayed in the output to keep track of the progression of a process.
        Function is called everytime the process progresses (to update the bar)

        Arguments:
            name:       What to label the bar as in the output (str)
            value:      Current number of total processes completed (int)
            endvalue:   Total number of processes to complete - for the bar to
                        reach 100% (int)
            bar_length: How long to print the bar (number of "-") (int)
            width:      Sets alignment space for the bar
        Returns:
            None
        Raises:
            None
        """
        # Check arguments are of the correct type
        if not isinstance(name, str):
            raise TypeError(
                "'name' argument must be passed as a string"
            )
        if not (
            isinstance(value, (float, int)) or
            isinstance(endvalue, (float, int))
        ):
            raise TypeError(
                "'value' and 'end' arguments must be passed as floats or ints"
            )
        if not (
            isinstance(bar_length, int) or
            isinstance(width, int)
        ):
            raise TypeError(
                "'bar_length' and 'width' arguments must be passed as ints"
            )

        # Check arguments are of the correct values
        if value > endvalue:
            raise ValueError(
                "Current value cannot exceed the end value"
            )

        percent = float(value) / endvalue
        arrow = '-' * int(round(percent*bar_length) - 1) + '>'
        spaces = ' ' * (bar_length - len(arrow))
        up_color = ""
        if uploading:
            up_color = "\033[1;31m"
        sys.stdout.write(
            "\r{9}{0: <{1}} : [{2}]{3}%   ({4} / {5}) \033[1;36m ~~{6} rows uploaded in {7} batches~~ {8}% of memory used\033[0;0m".format(
                name,
                width,
                arrow + spaces,
                int(round(percent*100)),
                value,
                endvalue,
                rows,
                batches,
                memory,
                up_color
            )
        )
        sys.stdout.flush()
        if value == endvalue:
            sys.stdout.write('\n\n')