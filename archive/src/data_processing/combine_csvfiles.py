import os
import csv
import pandas as pd
from typing import List
from datetime import datetime
import gcsfs


class XbrlCsvAppender:
    """ This class combines the csvs. """

    def combine_csv_pd(self, files: List[str], outfile: str):
        """
        Combines all csv files listed in files into a single csv file using
        pandas.

        Arguments:
            files:     List of the file paths of the files to be combined
                       (list)
            outfile:   Filepath to write combined csv file (str)
        Returns:
            None
        Raises:
            None
        """
        # Check arguments are of correct types
        if not all(isinstance(file, str) for file in files):
            raise TypeError(
                "The csv paths you have specified need to be passed as \
                strings"
            )
        if not isinstance(outfile, str):
            raise TypeError(
                "The output filepath needs to be specified as a string"
            )

        # Check file paths are valid
        if not all(os.path.exists(file) for file in files):
            raise ValueError(
                "Not all of the input csv files have valid file paths"
            )
        if not all(file[-4:] == ".csv" for file in files):
            raise ValueError(
                "Not all of the input files are csv's."
            )

        # WARNING: Causes memory overuse for large files
        combined_csv = pd.concat([pd.read_csv("gs://"+f,
                                              index_col=None,
                                                header=0,
                                                sep=",",
                                                lineterminator="\t",
                                                quotechar='"',
                                              low_memory=False)
                                  for f in files])
        combined_csv.to_csv("gs://"+outfile, index=False)

    @staticmethod
    def _add_path(indir: str, files: str):
        """
        Returns a list with the paths appended to file names.

        Arguments:
            indir:   List of csv files specified by user (list)
            files:   Filepath to write combined csv file (str)
        Returns:
            None
        Raises:
            None
        """

        # Check arguments are of the correct types
        if not isinstance(indir, str):
            raise TypeError(
                "input directory must be specified as a string"
            )
        if not all(isinstance(file, str) for file in files):
            raise TypeError(
                "All files must be specified as a string"
            )

        # Check arguments are the correct values
        if not os.path.exists(indir):
            raise ValueError(
                "Specified input directory does not exist"
            )
            
        return [indir+i for i in files]

if __name__ == '__main__':

    indir = 'ons-companies-house-dev-parsed-csv-data'
    outdir = 'ons-companies-house-dev-outputs-data/appender_test'
    year = 2010
    quarter = "None"

    fs = gcsfs.GCSFileSystem("ons-companies-house-dev",
                             token = "/home/dylan_purches/Desktop/data_key.json",
                             cache_timeout=1)

    appender = XbrlCsvAppender(fs)
    appender.merge_files_by_year(indir, outdir, year, quarter)
