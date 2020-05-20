import os
from os import listdir
from os.path import isfile, join
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
from scrapy.item import Item, Field
from scrapy.http import Request, Response

from scrapy.pipelines.files import FilesPipeline
from urllib.parse import urlparse
import requests
import hashlib

class MyItem(Item):
    file_urls = Field()
    files = Field()

class XBRLSpider(CrawlSpider):

    name = "xbrl_scraper"

    # custom_settings = {
    #      'ITEM_PIPELINES' : {'scrapy.pipelines.files.FilesPipeline': 1},
    #      'FILES_STORE' : "/shares/data/20200519_companies_house_accounts/xbrl_scraped_data_testing/"
    # }

    allowed_domains = ['download.companieshouse.gov.uk/en_accountsdata.html']
    start_urls = ['http://download.companieshouse.gov.uk/en_accountsdata.html']

    filepath = "/shares/data/20200519_companies_house_accounts/xbrl_scraped_data_testing/full"

    def parse(self, response):

        # Get a list of all filenames excluding directories and file extensions
        files = [f.split(".")[0] for f in listdir(self.filepath) if isfile(join(self.filepath, f))]

        # Extract all links from the web page (the response)
        links = response.xpath('//body//a/@href').extract()

        # Trim out links which do not point to zip files and make the URLs absolute
        links = [response.urljoin(link) for link in links if link.split('.')[-1] == "zip"]

        # Filter list of links to exclude those whose files have already been downloaded
        # This is based on a comparison between the existing files which have SHA1 hashed filenames
        # and the SHA1 hashes of the scraped URLs
        filtered_links = [link for link in links if hashlib.sha1(link.encode('utf-8')).hexdigest() not in files]

        # Yield items for download
        for link in filtered_links:
            yield MyItem(file_urls=[link])

        #item = MyItem(file_urls=['http://download.companieshouse.gov.uk/Accounts_Bulk_Data-2020-05-19.zip'])

        # 0c393a225a7afbfaa3f6e7bb7387da19af85f6ec
        # This is a hash of 'http://download.companieshouse.gov.uk/Accounts_Bulk_Data-2020-05-19.zip'