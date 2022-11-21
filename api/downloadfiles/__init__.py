import logging
import wget
import scrapy
from bs4 import BeautifulSoup

import azure.functions as func
import scrapy
from scrapy.crawler import CrawlerProcess
import re

class QuotesToCsv(scrapy.Spider):
    """scrape first line of  quotes from `wikiquote` by 
    Maynard James Keenan and save to json file"""
    name = "MJKQuotesToCsv"
    start_urls = [
        'https://en.wikiquote.org/wiki/Maynard_James_Keenan',
    ]
    custom_settings = {
        'ITEM_PIPELINES': {
            '__main__.ExtractFirstLine': 1
        },
        'FEEDS': {
            'quotes.csv': {
                'format': 'csv',
                'overwrite': True
            }
        }
    }

    def parse(self, response):
        """parse data from urls"""
        for quote in response.css('div.mw-parser-output > ul > li'):
            yield {'quote': quote.extract()}


class ExtractFirstLine(object):
    def process_item(self, item, spider):
        """text processing"""
        lines = dict(item)["quote"].splitlines()
        first_line = self.__remove_html_tags__(lines[0])

        return {'quote': first_line}

    def __remove_html_tags__(self, text):
        """remove html tags from string"""
        html_tags = re.compile('<.*?>')
        return re.sub(html_tags, '', text)

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
        process = CrawlerProcess()
        process.crawl(QuotesToCsv)
        process.start()

        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
