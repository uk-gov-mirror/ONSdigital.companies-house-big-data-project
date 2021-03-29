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

        files = [filename for filename in self.fs.ls(directory)
                        if ((".htm" in filename.lower())
                            or (".xml" in filename.lower()))
                    ]

        print(files)

if __name__ == "__main__":

    # Create an instance of GCPAuthenitcator and generate keys
    authenticator = GCPAuthenticator(auth_setup["iam_key"], auth_setup["project_id"])
    authenticator.get_sa_keys(auth_setup["keys_fp"])

    directory = "ons-companies-house-dev-xbrl-unpacked-data/v1_unpacked_data"

    batcher = XbrlBatcher(authenticator)
    batcher.batch_files(directory)

    authenticator.clean_up_keys()