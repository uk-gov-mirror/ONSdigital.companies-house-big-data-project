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
from google.cloud import storage

class XbrlScraper:

if __name__ == "__main__":
    scraper = XbrlScraper()

    url = "http://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
    base_url = "http://download.companieshouse.gov.uk/"

    # url = "http://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html"
    # base_url = "http://download.companieshouse.gov.uk/"

    dir_to_save = "ons-companies-house-dev-xbrl-scraped-data/requests_scraper_test_folder"

    scraper.scrape_webpage(url, base_url, dir_to_save)
