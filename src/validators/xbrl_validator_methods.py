from os import listdir, mkdir
from os.path import isfile, join, exists, getsize

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

                files = [filepath + f for f in listdir(file_source)]

                for file in files:
                    print("File: " + file)
                    print("File size: " + str(getsize(file)))
            else:
                print("Specified filepath is not a directory!")

        else:
            print("Specified directory does not exist!")