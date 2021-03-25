import os
import requests
from bs4 import BeautifulSoup
import time
import random
import base64
from google.cloud import storage

def scrape_webpage(event, context):
    dir_to_save = "ons-companies-house-dev-xbrl-scraped-data"

    storage_client = storage.Client()
    bucket = storage_client.bucket(dir_to_save.split("/")[0])

    zip_url = event["attributes"]["zip_path"]
    link = event["attributes"]["link_path"]

    if len(dir_to_save.split("/")[1:]) > 0:
        blob = bucket.blob("/".join(dir_to_save.split("/")[1:]) + "/" + link)
    else:
        blob = bucket.blob(link)
    
    print("Downloading " + link + "...")
    zip_file = requests.get(zip_url).content
    
    print("Saving zip file " + link + "...")
    blob.upload_from_string(zip_file, content_type="application/zip")
