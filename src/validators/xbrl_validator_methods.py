from os import listdir, walk
from os.path import isfile, exists, getsize, getmtime, join
from datetime import datetime
import zipfile
import gcsfs


class XbrlValidatorMethods:
    """This is a class that validates the XBRL data files."""

    def __init__(self, fs):
        self.__init__
        self.fs = fs

    def validate_compressed_files(self, filepath):
        """
        Validates and prints information, including name size and date
        modified, on files in specified directory

        Arguments:
            filepath: source directory of files of interest (str)
        Returns:
            None
        Raises:
            None
        """
        # Check argument is of the correct type
        if not isinstance(filepath, str):
            raise TypeError(
                "The output filepath needs to be specified as a string"
            )
        # Check input is a valid file path
        if not exists(filepath) :
            raise ValueError(
                "The file path specified does not exist"
            )

        if self.fs.exists(filepath):
            if not self.fs.isfile(filepath):

                files = self.fs.ls(filepath)
                files = [f for f in files if f.split(".")[-1] == "zip"]
                #[filepath + "/" + f for f in listdir(filepath)]

                for file in files:
                    gcs_file = self.fs.open(file, 'rb')
                    print("File: " + file)
                    print("File size: " + str(gcs_file.info()["size"]) + " bytes")

                    #file_time = datetime.utcfromtimestamp(gcs_file.info()["updated"])
                    print("File modified: "
                          + gcs_file.info()["updated"])
            else:
                print("Specified filepath is not a directory!")

        else:
            print("Specified directory does not exist!")
