from os import listdir
from os.path import isfile, exists, getsize, getmtime
from datetime import datetime

class XbrlValidatorMethods:

    def __init__(self):
	    self.__init__

    @staticmethod
    def validate_compressed_files(filepath):
        """
            Validates and returns information on files in specified directory

            Arguments:
                filepath: source directory of files of interest
            Returns:
                None
            Raises:
                None
            """

        if exists(filepath):
            if not isfile(filepath):

                files = [filepath + f for f in listdir(filepath)]

                for file in files:
                    print("File: " + file)
                    print("File size: " + str(getsize(file)) + " bytes")

                    file_time = datetime.utcfromtimestamp(getmtime(file))
                    print("File modified: " + file_time.strftime("%Y-%m-%d %H:%M:%S.%f+00:00 (UTC)"))
            else:
                print("Specified filepath is not a directory!")

        else:
            print("Specified directory does not exist!")

# myobj = XbrlValidatorMethods()
# file_source = "/home/peterd/repos/companies_house_accounts/data/for_testing/xbrl_data/"
# myobj.validate_compressed_files(file_source)