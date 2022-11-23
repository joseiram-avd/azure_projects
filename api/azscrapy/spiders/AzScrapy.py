import scrapy

# import logging
# import wget
# import scrapy
# from bs4 import BeautifulSoup

# import azure.functions as func

# from scrapy.crawler import CrawlerProcess
# from scrapy.crawler import CrawlerRunner
import re
# from scrapy.pipelines.files import FilesPipeline
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

# from  shared_code.pipelines import DownfilesPipeline #(relative)
from azscrapy.items import DownfilesItem
import os
from urllib.parse import urlparse
from scrapy.utils.project import get_project_settings

class AzScrapy(scrapy.Spider):
    name = 'AzScrapy'
    allowed_domains = ['dados.antt.gov.br']
    start_urls = ['https://dados.antt.gov.br/dataset/veiculos-habilitados']
    update_settings = get_project_settings()

    custom_settings = {
            'ITEM_PIPELINES': {
                'azscrapy.pipelines.AzScrapyPipeline': 1,
            },
            'FILES_STORE' : 'C:/nirsoft'
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        if(crawler.settings.frozen):
            crawler.settings.frozen = False
            crawler.settings.set("SETTING","NEW_VALUE")
            #crawler.settings.overrides.settings.set("SETTING","NEW_VALUE")
            crawler.settings.freeze()
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        return spider
    
    @classmethod
    def update_settings(self):
        pass

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_link, dont_filter=True)

    def parse_link(self, response):
        links = set( LinkExtractor( allow=('dataset'), canonicalize=True, unique=True).extract_links(response) )
        for link in links:
            yield scrapy.Request(link.url, callback=self.parse_item, dont_filter=True)

    def parse_item(self, response):
        file_url = response.css('.resource-url-analytics::attr(href)').get()
        file_url = response.urljoin(file_url)
        file_extension = file_url.split('.')[-1]
        if file_extension not in ('pdf', 'json', 'csv'):
            return
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        item['original_file_name'] = file_url.split('/')[-1]
        if '2020' in item['original_file_name']:
            yield item
