from google.oauth2 import service_account
import googleapiclient.discovery
import git
import json

import os
import shutil

class GCPAuthenticator:
    """Class for downloading keys relevant to pipeline modules"""
    def __init__(self, iam_access_sa, project):
        
        self.iam_access_sa = iam_access_sa
        self.project = project

        self.master_key = None
        self.xbrl_scraper_key = None
        self.xbrl_unpacker_key = None
        self.xbrl_validator_key = None
        self.xbrl_parser_key = None

    def get_sa_keys(self, keys_filepath):
        
        if not os.path.isdir(keys_filepath):
            raise ValueError(
        "Specified path either does not exist or isn't a directory"
        )
        if self.check_git_repo(keys_filepath):
            raise ValueError(
        "SECURITY WARNING - do not save keys within a git repo! Ensure folder is immediately deleted from repo"
        )   

        if os.path.exists(keys_filepath + "/master_key.json"):
            master_key = self.read_json(keys_filepath + "/master_key.json")
            self.master_key = master_key
            self.xbrl_scraper_key = master_key
            self.xbrl_unpacker_key = master_key
            self.xbrl_validator_key = master_key
            self.xbrl_parser_key = master_key

        else:
            if os.path.exists(keys_filepath + "/xbrl_scraper_key.json"):
                self.xbrl_scraper_key = self.read_json(keys_filepath + "/xbrl_scraper_key.json")
            else:
                print("Downloading xbrl scraper key")
                self.xbrl_scraper_key = self.create_key("xbrl-web-scraper@ons-companies-house-dev.iam.gserviceaccount.com")

            if os.path.exists(keys_filepath + "/xbrl_unpacker_key.json"):
                self.xbrl_unpacker_key = self.read_json(keys_filepath + "/xbrl_unpacker_key.json")
            else:
                self.xbrl_unpacker_key = self.create_key("xbrl-unpacker@ons-companies-house-dev.iam.gserviceaccount.com")

            if os.path.exists(keys_filepath + "/xbrl_validator_key.json"):
                self.xbrl_validator_key = self.read_json(keys_filepath + "/xbrl_validator_key.json")
            else:
                self.xbrl_validator_key = self.create_key("xbrl-validator@ons-companies-house-dev.iam.gserviceaccount.com")

            if os.path.exists(keys_filepath + "/xbrl_parser_key.json"):
                self.xbrl_parser_key = self.read_json(keys_filepath + "/xbrl_parser_key.json")
            else:
                self.xbrl_parser_key = self.create_key("xbrl-parser@ons-companies-house-dev.iam.gserviceaccount.com")


    def create_key(self, service_account_email):
        """Creates a key for a service account."""

        credentials = service_account.Credentials.from_service_account_file(
            filename=self.iam_access_sa,
            scopes=['https://www.googleapis.com/auth/cloud-platform'])

        service = googleapiclient.discovery.build(
            'iam', 'v1', credentials=credentials)

        key = service.projects().serviceAccounts().keys().create(
            name='projects/-/serviceAccounts/' + service_account_email, body={}
            ).execute()

        print('Created key: ' + key['name'])
        return key
    
    def delete_key(self, key):
        """Deletes a service account key."""
        try:
            full_key_name = key["name"]
        except:
            full_key_name = "projects/" + key["project_id"] + "/serviceAccounts/" + key["client_email"] + "/keys/" + key["private_key_id"]


        credentials = service_account.Credentials.from_service_account_file(
            filename= self.iam_access_sa,
            scopes=['https://www.googleapis.com/auth/cloud-platform'])

        service = googleapiclient.discovery.build(
            'iam', 'v1', credentials=credentials)

        service.projects().serviceAccounts().keys().delete(
            name=full_key_name).execute()

        print('Deleted key: ' + full_key_name)

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
        json_object = json.loads(data)

        return json_object



if __name__ == "__main__":

    keys_folder = "/home/dylan_purches/repos/companies-house-big-data-project/test_dir"

    authenticator = GCPAuthenticator("/home/dylan_purches/keys/data_key.json", "ons-companies-house-dev")

    authenticator.get_sa_keys("/home/dylan_purches/keys")