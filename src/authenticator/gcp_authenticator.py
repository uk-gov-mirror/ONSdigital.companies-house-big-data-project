from google.oauth2 import service_account
import googleapiclient.discovery
import git
import json
import base64
import time

import os
import shutil

class GCPAuthenticator:
    """Class for downloading keys relevant to pipeline modules"""
    def __init__(self, iam_access_sa, project):
        
        self.iam_access_sa = iam_access_sa
        self.project = project

        self.keys_filepath = None

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

        self.keys_filepath = keys_filepath

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
                self.xbrl_parser_key = keys_filepath + "/xbrl_parser_key.json"
            else:
                parser_dict = self.create_key("xbrl-parser@ons-companies-house-dev.iam.gserviceaccount.com")
                with open(keys_filepath + "/xbrl_parser_key.json", "w") as f:
                    json.dump(parser_dict, f)
                self.xbrl_parser_key = keys_filepath + "/xbrl_parser_key.json"
        
        time.sleep(5)


    def create_key(self, service_account_email):
        """Creates a key for a service account."""

        credentials = service_account.Credentials.from_service_account_file(
            filename=self.iam_access_sa)
            #scopes=['https://www.googleapis.com/auth/cloud-platform'])

        service = googleapiclient.discovery.build(
            'iam', 'v1', credentials=credentials)

        keys_list = service.projects().serviceAccounts().keys().list(
            name='projects/-/serviceAccounts/' + service_account_email).execute()
        
        if len(keys_list["keys"]) >= 10:
            raise ValueError(
        "Too many keys have been generated for this service account. Please remove some and try again"
        )

        response_json = service.projects().serviceAccounts().keys().create(
            name='projects/-/serviceAccounts/' + service_account_email, body={}
            ).execute()

        print('Created key: ' + response_json['name'])

        key = eval(base64.b64decode(response_json["privateKeyData"]))

        return key
    
    def delete_key(self, key):
        """Deletes a service account key."""
            
        full_key_name = "projects/" + key["project_id"] + "/serviceAccounts/" + key["client_email"] + "/keys/" + key["private_key_id"]


        credentials = service_account.Credentials.from_service_account_file(
            filename= self.iam_access_sa,
            scopes=['https://www.googleapis.com/auth/cloud-platform'])

        service = googleapiclient.discovery.build(
            'iam', 'v1', credentials=credentials)

        service.projects().serviceAccounts().keys().delete(
            name=full_key_name).execute()

        print('Deleted key: ' + full_key_name)

    def clean_up_keys(self, remove_disk_keys=True):
        print("CLEANING UP")
        keys_names = [name for name in self.__dict__.keys() if (name.split("_")[-1] == "key" and name.split("_")[0] != "master")]
        print(keys_names)
        for k in keys_names:
            if (self.__dict__[k] != None) and type(self.__dict__[k]) == dict:
                self.delete_key(self.__dict__[k])
                self.__dict__[k] = None
            elif remove_disk_keys and type(self.__dict__[k]) == str:
                print("Removing disk key")
                self.delete_key(self.read_json(self.keys_filepath + "/" + k + ".json"))
                os.remove(self.keys_filepath + "/" + k + ".json")
                

    def remove_n_keys(self, sa_email, n=10):
            credentials = service_account.Credentials.from_service_account_file(
                filename=self.iam_access_sa,
                scopes=['https://www.googleapis.com/auth/cloud-platform'])

            service = googleapiclient.discovery.build(
                'iam', 'v1', credentials=credentials)

            keys_response = service.projects().serviceAccounts().keys().list(
                name='projects/-/serviceAccounts/' + sa_email).execute()
            keys_list = keys_response["keys"]
            keys_list.reverse()
            
            count = min(n, len(keys_list))
            for i in range(count):
                self.delete_key(keys_list[i])
                print("DELETED A KEY")


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
    import atexit
    import gcsfs
    

    keys_folder = "/home/dylan_purches/repos/companies-house-big-data-project/test_dir"
    authenticator = GCPAuthenticator("/home/dylan_purches/keys/data_key.json", "ons-companies-house-dev")
    
    authenticator.get_sa_keys("/home/dylan_purches/keys")

    fs = gcsfs.GCSFileSystem(token=authenticator.xbrl_parser_key, cache_timeout=0)
    print(fs.ls("ons-companies-house-dev-xbrl-unpacked-data/scraper_test/Accounts_Monthly_Data-April2019"))