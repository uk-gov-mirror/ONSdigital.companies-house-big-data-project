from os import listdir, walk
from os.path import isfile, exists, getsize, getmtime, join
from datetime import datetime
import zipfile

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

                files = [filepath + "/" + f for f in listdir(filepath)]

                for file in files:
                    print("File: " + file)
                    print("File size: " + str(getsize(file)) + " bytes")

                    file_time = datetime.utcfromtimestamp(getmtime(file))
                    print("File modified: " + file_time.strftime("%Y-%m-%d %H:%M:%S.%f+00:00 (UTC)"))
            else:
                print("Specified filepath is not a directory!")

        else:
            print("Specified directory does not exist!")

    @staticmethod
    def validate_extracted_files(filepath_to_zip_files, filepath_to_extracted_files):
        """
        Validates and returns information on files in specified directory
        To be used for validating files which have been extracted from a zip file

        Arguments:
            filepath_to_zip_files: source directory of files of interest
            filepath_to_extracted_files: destination directory of files of interest
        Returns:
            None
        Raises:
            None
        """

        def get_size_of_folder(start_path):
            """
            Recursive method to return the combined size of a folder and its sub-directories

            Arguments:
                start_path: directory to be checked
            Returns:
                total_size: combined size of the directory and its sub-directories
            Raises:
                None
            """

            total_size = 0
            for path, dirs, files in walk(start_path):
                for f in files:
                    fp = join(path, f)
                    total_size += getsize(fp)

            return total_size

        # If both filepaths to the zip files and the extracted files exist...
        if exists(filepath_to_zip_files) and exists(filepath_to_extracted_files):

            zip_files = [filepath_to_zip_files + "/" + f for f in listdir(filepath_to_zip_files)]
            extracted_files = [filepath_to_extracted_files + "/" + f
                               for f in listdir(filepath_to_extracted_files) if not isfile(f)]

            zip_files.sort()
            extracted_files.sort()

            for z, e in zip(zip_files, extracted_files):

                print("Validating zip file: " + z)
                print("Validating extracted folder: " + e)

                # Zip file -> extracted folder size comparison
                zip_size = getsize(z)
                extracted_folder_size = get_size_of_folder(e)

                print("Zip file size: " + str(zip_size) + " bytes")
                print("Extracted folder size: " + str(extracted_folder_size) + " bytes")
                if extracted_folder_size >= zip_size:
                    print("Extracted folder size larger than original zip file - size check OK")
                else:
                    print("Extracted folder size smaller than original zip file - size check failed!")

                # Check to see if all files have been extracted correctly
                files_in_zip = set(zipfile.ZipFile(z).namelist())
                files_in_extracted_folder = set([f for f in listdir(e)])
                diff = files_in_zip.difference(files_in_extracted_folder)

                if not diff:
                    print("All files extracted correctly from zip file")
                else:
                    print("Some files not extracted correctly from zip file!:")
                    print(str(diff))

        else:
            if not exists(filepath_to_zip_files):
                print("Specified directory to zip files does not exist!")
            if not exists(filepath_to_extracted_files):
                print("Specified directory to extracted files does not exist!")

#myobj = XbrlValidatorMethods()
#file_source = "/home/peterd/repos/companies_house_accounts/data/for_testing/xbrl_data"
#file_dest = "/home/peterd/repos/companies_house_accounts/data/for_testing/xbrl_data_extracted"
#myobj.validate_extracted_files(file_source, file_dest)