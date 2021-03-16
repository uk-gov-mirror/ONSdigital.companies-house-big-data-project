from google.oauth2 import service_account
import googleapiclient.discovery
import git
import json

import os
import shutil

class GCPAuthenticator:
    """Class for downloading keys relevant to pipeline modules"""
    def __init__(self):
        self.master_key = None
        self.xbrl_scraper_key = None
        self.xbrl_unpacker_key = None
        self.xbrl_parser_key = None

    def get_sa_keys(self, keys_filepath, project, get_keys_sa):
        
        if not os.path.isdir(keys_filepath):
            raise ValueError(
        "Specified path either does not exist or isn't a directory"
        )
        if self.check_git_repo(keys_filepath):
            raise ValueError(
        "SECURITY WARNING - do not save keys within a git repo! Ensure folder is immediately deleted from repo"
        )   

        keys_to_download = []
        if os.path.exists(keys_filepath + "/master_key.json"):
            master_fp = self.read_json(keys_filepath + "/master_key.json")
            self.master_key = master_fp
            self.xbrl_scraper_key = master_fp
            self.xbrl_unpacker_key = master_fp
            self.xbrl_parser_key = master_fp
        else:
            if os.path.exists(keys_filepath + "/xbrl_scraper_key.json"):
                self.xbrl_scraper_key = self.read_json(keys_filepath + "/xbrl_scraper_key.json")
            else:
                keys_to_download.append("xbrl_scraper_key")

            if os.path.exists(keys_filepath + "/xbrl_unpacker_key.json"):
                self.xbrl_unpacker_key = self.read_json(keys_filepath + "/xbrl_unpacker_key.json")
            else:
                keys_to_download.append("xbrl_unpacker_key")

            if os.path.exists(keys_filepath + "/xbrl_parser_key.json"):
                self.xbrl_parser_key = self.read_json(keys_filepath + "/xbrl_parser_key.json")
            else:
                keys_to_download.append("xbrl_unpacker_key")
        


    @staticmethod
    def create_key(service_account_email):
        """Creates a key for a service account."""

        credentials = service_account.Credentials.from_service_account_file(
            filename="/home/dylan_purches/keys/data_key.json",
            scopes=['https://www.googleapis.com/auth/cloud-platform'])

        service = googleapiclient.discovery.build(
            'iam', 'v1', credentials=credentials)

        key = service.projects().serviceAccounts().keys().create(
            name='projects/-/serviceAccounts/' + service_account_email, body={"lifetime":"300"}
            ).execute()

        print('Created key: ' + key['name'])
        return key

    @staticmethod
    def check_git_repo(filepath):
        if os.path.exists(filepath):
            try:
                repo = git.Repo(filepath, search_parent_directories=True)
                is_git = True
            except:
                is_git = False
        else:
            raise ValueError("Specified filepath does not exist")
        return is_git

    @staticmethod
    def read_json(filepath):

        if not os.path.isfile(filepath):
            raise ValueError(
        "Specified filepath does not exist"
        )
        if filepath.split(".")[-1] != "json":
            raise ValueError(
        "Please specify the path of a JSON file"
        )

        with open(filepath, 'r') as f:
            data = f.read()
        json_object = json.load(data)

        return json_object



if __name__ == "__main__":

    keys_folder = "/home/dylan_purches/repos/companies-house-big-data-project/test_dir"

    authenticator = GCPAuthenticator()