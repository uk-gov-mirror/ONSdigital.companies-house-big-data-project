# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.pipelines.files import FilesPipeline

class XbrlScraperPipeline(FilesPipeline):

    def file_path(self, request, response=None, info=None):
        url = request.url
        filename = request.url.split("/")[-1]
        print("URL:")
        print(url)
        return filename
