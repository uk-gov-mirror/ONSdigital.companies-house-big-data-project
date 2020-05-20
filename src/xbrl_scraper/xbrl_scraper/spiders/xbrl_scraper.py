import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
from scrapy.item import Item, Field
#from scrapy.http import Request

import requests

class MyItem(Item):
    url = Field()

class XBRLSpider(CrawlSpider):

    name = "xbrl_scraper"

    allowed_domains = ['download.companieshouse.gov.uk/en_accountsdata.html']
    start_urls = ['http://download.companieshouse.gov.uk/en_accountsdata.html']

    filepath = "/shares/data/20200519_companies_house_accounts/xbrl_scraped_data_testing/"

    def parse(self, response):

        print("hello")

        links = response.xpath('//body//a/@href').extract()

        # Trim out links which do not point to zip files
        links = [link for link in links if link.split('.')[-1] == "zip"]

        #print(links)

        for link in range(0, len(links)):

            links[link] = response.urljoin(links[link])

        #links = [links[0]]

        print(links)
        links = [links[0]]
        print(links)

        #filepath = "/shares/data/20200519_companies_house_accounts/xbrl_scraped_data_testing/"
        filename = self.filepath + "test.zip"

        #r = Request(links[0])

        r = requests.get(links[0])

        #print(r.body)

        with open(filename, "wb") as f:
            f.write(r.content)

            #f.close()

        item = MyItem()
        item['url'] = links[0]

        return item