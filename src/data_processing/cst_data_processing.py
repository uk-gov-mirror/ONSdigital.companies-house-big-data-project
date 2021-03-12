# import cv2
from os import listdir, mkdir
from os.path import isfile, join, exists, splitext
import random
import zipfile
import shutil
import io


class DataProcessing:
    """ This is a class for processing data. """

    def __init__(self, fs):
        self.__init__
        self.fs = fs

    def extract_compressed_files(self, file_source, file_dest):
        """
        Extracts .zip files from a given directory or filename
        to a given file directory.
        Arguments:
            file_source: source of files of interest, either a filename
                         or a directory (str)
            file_dest: destination directory to save extracted files to (str)
        Returns:
            None
        Raises:
            None
        """
        if self.fs.exists(file_dest):
            if self.fs.exists(file_source):

                # Retrieve a list of all zip files that have been extracted
                # If no such list exists, create one
                if self.fs.exists(file_dest + "/extracted_files.txt"):
                    extracted_files_txt = self.fs.open(
                        file_dest + "/extracted_files.txt", "r")
                    list_extracted_files = [f.rstrip("\n") for f in
                                            extracted_files_txt]
                    extracted_files_txt.close()
                else:
                    list_extracted_files = []
                extracted_files_txt = self.fs.open(
                    file_dest + "/extracted_files.txt", "a")

                if self.fs.isfile(file_source):
                    files = [file_source]
                else:
                    # Retrieve list of all files present in the directory
                    # (not folders)
                    files = self.fs.ls(file_source)
                    #[file_source + f for f in listdir(file_source)]
                    files = [f for f in files if self.fs.isfile(f)]

                # Filter out all files from 'files' that have already been
                # extracted
                files = [f for f in files if f not in list_extracted_files]

                print("Extracting files...")
                for file in files:
                    if file.endswith('.zip'):

                        directory = file_dest + "/" + \
                                    file.split('.')[0].split("/")[-1]

                        # If the directory for the zip contents exists already,
                        # delete it just in case a previous attempt at
                        # extraction was incomplete. Otherwise, create a new
                        # directory
                        if self.fs.exists(directory):
                            self.fs.rm(directory, recursive=True)

                        # Perform the extraction
                        with zipfile.ZipFile(self.fs.open(file), 'r') \
                                as zip_ref:
                            for contentfilename in zip_ref.namelist():
                                contentfile = zip_ref.read(contentfilename)
                                try:
                                    with self.fs.open(
                                            directory + "/" + contentfilename,
                                            'wb') as f:
                                        f.write(contentfile)

                                    self.fs.setxattrs(
                                        directory + "/" + contentfilename,
                                        content_type="text/"
                                                     +contentfilename \
                                                         .split(".")[-1])
                                except:
                                    print("Failed to save:", contentfilename)
                        print("Extracted files from " + file)

                        # Add extracted filename to file list
                        extracted_files_txt.write(file + "\n")
                    else:
                        print("File extension " + file.split('.')[
                            1] + " not supported")
                        print("Unable to extract file " + file)

                extracted_files_txt.close()
                print("Extraction complete")
            else:
                print("File or directory not found: " + file_source)
        else:
            print("Destination directory not valid!: " + file_dest)
