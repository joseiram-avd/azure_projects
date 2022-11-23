import logging
import wget
import scrapy
from bs4 import BeautifulSoup

import azure.functions as func
 
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
import re
from scrapy.pipelines.files import FilesPipeline
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from azscrapy.spiders import AzScrapy
from scrapy.utils.project import get_project_settings

import os
from urllib.parse import urlparse

# Reactor restart

from crochet import setup, wait_for

# setup()

# class NirsoftSpider(CrawlSpider):
#     name = 'nirsoft'
#     allowed_domains = ['dados.antt.gov.br']
#     start_urls = ['https://dados.antt.gov.br/dataset/veiculos-habilitados']

#     # allowed_domains = ['www.nirsoft.net']
#     # start_urls = ['http://www.nirsoft.net/']
   
#     # 'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
#     # scrapy.pipelines.files.FilesPipeline
#     #  '__main__.DownfilesItem': 1

#     custom_settings = {
#         'ITEM_PIPELINES': {
#             'shared_code.pipelines.DownfilesPipeline': 1,
            
#         },
#         'FILES_STORE' : 'C:/nirsoft'
#     }

#     rules = (
#         Rule(LinkExtractor(allow=r'dataset/'),
#         callback='parse_item', follow = True),
#     ) 

#     def parse_item(self, response):
#         file_url = response.css('.resource-url-analytics::attr(href)').get()
#         file_url = response.urljoin(file_url)
#         file_extension = file_url.split('.')[-1]
#         if file_extension not in ('zip', 'json', 'csv'):
#             return
#         item = DownfilesItem()
#         item['file_urls'] = [file_url]
#         item['original_file_name'] = file_url.split('/')[-1]
#         yield item

# class DownfilesItem(scrapy.Item):
# 	# define the fields for your item here like:
# 	file_urls = scrapy.Field()
# 	original_file_name = scrapy.Field()
# 	files = scrapy.Field

def run_spider():
    
    # crawler = CrawlerProcess(settings=settings)
    # d = crawler.crawl(AzScrapy)
 
    process = CrawlerProcess(settings={
            'ITEM_PIPELINES': {
                'azscrapy.pipelines.AzScrapyPipeline': 1,
            },
            'FILES_STORE' : 'C:/nirsoft'} )
 
    process.crawl(AzScrapy)
    process.start()

    return end_spider() 

# @wait_for(10)
def end_spider():
    return func.HttpResponse(
            "1 This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
    )

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        # process = CrawlerProcess()
        # process.crawl(NirsoftSpider)
        # process.start()

        # crawler = CrawlerRunner()
        # crawler.crawl(NirsoftSpider)
 
        return run_spider()

        # return func.HttpResponse(
        #     "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
        #     status_code=200
        # )


