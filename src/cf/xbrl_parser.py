import configparser
#import sys

#sys.path.append('~/companies-house-big-data-project/src')

from src.authenticator.gcp_authenticator import GCPAuthenticator
import gcsfs

# Reads from the config file
cfg = configparser.ConfigParser()
cfg.read("cha_pipeline.cfg")

# Config imports
auth_setup = cfg["auth_setup"]

class XbrlBatcher:
    """ This is a class for parsing the XBRL data."""

    def __init__(self, auth):
        self.__init__
        self.project = auth.project
        self.key = auth.xbrl_parser_key
        self.fs = gcsfs.GCSFileSystem(
            project=self.project, token=self.key, cache_timeout=0)

    def batch_files(self, directory):

        batch_count_hard_limit = 360
        #batch_size_hard_limit = 4e9
        batch_size_hard_limit = 40000

        files = [filename for filename in self.fs.ls(directory)
                        if ((".htm" in filename.lower())
                            or (".xml" in filename.lower()))
                ]
        
        sizes = []
        num_files = 100
        #num_files = len(files) + 1
        for filename, n in zip(files[0:num_files], range(1, num_files + 1)):
            
            # if n % (int)(len(files) / 100) == 0:
            #     print("Retrieving file size for file " + str(n) + " of " + str(len(files)))
            
            sizes.append(self.fs.size(filename))

        print("File sizes...")
        print(sizes)

        dir_size = sum(sizes)
        print(dir_size)

        # Create list of files and their sizes and sort in descending order
        data = sorted(zip(files, sizes), key=lambda t: t[1], reverse=True)
        print(data)

        batch_list = [[]]
        batch_size_list = [[]]
        batch_size = 0
        batch_count = 0
        current_batch = 0
        for filename, size in data:
            size_remaining = batch_size_hard_limit - batch_size
            count_remaining = batch_count_hard_limit - batch_count

            print("---")
            print(size)
            print(size_remaining)

            # Start a new batch if the size or count of the current batch has hit the limit
            if (size_remaining - size <= 0) or (count_remaining <= 0):
                batch_list.append([])
                batch_size_list.append([])
                current_batch += 1
                batch_size = 0
                batch_count = 0

            batch_list[current_batch].append(filename)
            batch_size_list[current_batch].append(size)
            batch_size += size
            batch_count += 1

        print("Final batches:")
        [print(batch) for batch in batch_list]
        
        print("Batch sizes...")
        [print(sum(batch)) for batch in batch_size_list]

        