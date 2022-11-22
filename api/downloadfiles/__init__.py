import logging
import wget
import scrapy
from bs4 import BeautifulSoup

import azure.functions as func
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
import re
 
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

# Reactor restart

from crochet import setup, wait_for

setup()

class DownfilesItem(scrapy.Item):
	# define the fields for your item here like:
	file_urls = scrapy.Field()
	original_file_name = scrapy.Field()
	files = scrapy.Field

class NirsoftSpider(CrawlSpider):
    name = 'nirsoft'
    # allowed_domains = ['dados.antt.gov.br']
    # start_urls = ['https://dados.antt.gov.br/dataset/veiculos-habilitados']

    allowed_domains = ['www.nirsoft.net']
    start_urls = ['http://www.nirsoft.net/']

   
    # 'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
    custom_settings = {
        'ITEM_PIPELINES': {
            '__main__.DownfilesItem': 1
        },
        
        'FILES_STORE' : 'C:/nirsoft'
    }
    
    rules = (
        Rule(LinkExtractor(allow=r'utils/'),
        callback='parse_item', follow = True),
    ) 

    def parse_item(self, response):
        print( response )
        file_url = response.css('downloadline::attr(href)').get()
        file_url = response.urljoin(file_url)
        file_extension = file_url.split('.')[-1]
        if file_extension not in ('csv', 'json'):
            return
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        item['original_file_name'] = file_url.split('/')[-1]
        yield item

 
def run_spider():
    print( 'passei aqui ')
    crawler = CrawlerRunner()
    d = crawler.crawl(NirsoftSpider)

    return end_spider()

@wait_for(10)
def end_spider():
     
    return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
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
        return run_spider() 

