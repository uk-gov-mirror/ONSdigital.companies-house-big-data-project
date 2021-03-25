import base64
import os
import requests
from bs4 import BeautifulSoup
import time
import random
from google.cloud import storage, pubsub_v1

def collect_links(event, content):
    """
    Scrapes target web page and saves all zip files found to
    a directory
    Arguments:
        url:            Url of web page to scrape (str)
        base_url:       Base url of wep page to scrape (str)
        dir_save_to:    GCS directory to save zip files to, consisting of bucket_name/folder
    Returns:
        None
    Raises:
        None
    Notes:
        The base_url is needed as the links to the zip files
        are appended to this, not the html url
        
        Example:
        url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
        base_url = "http://download.companieshouse.gov.uk/"
        dir_to_save = "ons-companies-house-dev-xbrl-scraped-data/requests_scraper_test_folder"
    """
    url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
    base_url = "http://download.companieshouse.gov.uk/"
    dir_to_save = "ons-companies-house-dev-xbrl-scraped-data"
    
    print("Fetching content...")
    res = requests.get(url)

    #txt = res.text
    status = res.status_code
    
    # If the scrape was successfull, parse the contents
    if status == 200:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path("ons-companies-house-dev", "run_xbrl_web_scraper")

        soup = BeautifulSoup(res.content, "html.parser")
        links = soup.select("li")

        # Convert to string format
        links = [str(link) for link in links]

        # Extract filename from text if there is a downloadable file
        links = [link.split('<a href="')[1].split('">')[0] for link in links if "<a href=" in link]

        # Filter out files that are not zip
        links = [link for link in links if link[-4:] == ".zip"]

        storage_client = storage.Client()
        bucket = storage_client.bucket(dir_to_save.split("/")[0])

        # Download and save zip files
        for link in links:

            zip_url = base_url + link

            if "/" in link: 
                link = link.split("/")[-1]
                blob = bucket.blob("/".join(dir_to_save.split("/")[1:]) + "/" + link)
            else:
                blob = bucket.blob(link)

            # Only download and save a file if it doesn't exist in the directory
            if not blob.exists():
              data = "Zip file to download: {}".format(link).encode("utf-8")
              future = publisher.publish(
                topic_path, data, zip_path=zip_url, link_path=link
              )
              print(future.result())
            else:

                print(link + " already exists")
            time.sleep((random.random() * 2.0) + 3.0)