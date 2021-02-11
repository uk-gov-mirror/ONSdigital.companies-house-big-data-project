# -*- coding: utf-8 -*-
"""
Created on Mon Feb  1 09:59:54 2021

@author: derrip
"""

import os
import requests
from bs4 import BeautifulSoup
import time
import random
import gcsfs

class XbrlScraper:

    def __init__(self, fs):
        """
        Constructs all the necessary attributes for the XbrlScraper object of
        which there are none.
        """
        self.__init__
        self.fs = fs

    def scrape_webpage(self, url, base_url, dir_save_to):
        """
        Scrapes target web page and saves all zip files found to
        a directory

        Arguments:
            url:            Url of web page to scrape (str)
            base_url:       Base url of wep page to scrape (str)
            dir_save_to:    Directory to save zip files to
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
        """

        print("Fetching content...")
        res = requests.get(url)

        #txt = res.text
        status = res.status_code
        
        # If the scrape was successfull, parse the contents
        if status == 200:

            soup = BeautifulSoup(res.content, "html.parser")
            links = soup.select("li")

            # Convert to string format
            links = [str(link) for link in links]

            # Extract filename from text if there is a downloadable file
            links = [link.split('<a href="')[1].split('">')[0] for link in links if "<a href=" in link]

            # Filter out files that are not zip
            links = [link for link in links if link[-4:] == ".zip"]

            # Download and save zip files
            #for link in links:
            for link in [links[0]]:

                zip_url = base_url + link

                #filepath = os.path.join(dir_save_to, link)
                filepath = dir_save_to + "/" + link

                # Only download and save a file if it doesn't exist in the directory
                if not self.fs.exists(filepath):
                
                    print("Downloading " + link + "...")
                    zip_file = requests.get(zip_url).content
                    
                    print("Saving zip file " + link + "...")
                    with self.fs.open(filepath, 'wb') as fp:
                        fp.write(zip_file)

                    # Random sleep to avoid stressing the target server
                    time.sleep((random.random() * 2.0) + 3.0)

                else:

                    print(link + " already exists")

            print("All zip files saved")

        else:

            print("Unable to scrape web page!")
            print("Error code: " + status)

fs = gcsfs.GCSFileSystem(project="ons-companies-house-dev")

scraper = XbrlScraper(fs)

url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
base_url = "http://download.companieshouse.gov.uk/"
dir_to_save = "ons-companies-house-dev-test-xbrl-scraped-data/requests_scraper_test_folder"

#dir_to_save = r'/home/peter_derrick/shares/xbrl_scraped_data'

scraper.scrape_webpage(url, base_url, dir_to_save)
