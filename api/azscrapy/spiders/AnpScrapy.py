import scrapy

# import logging
# import wget
# import scrapy
# from bs4 import BeautifulSoup

# import azure.functions as func

# from scrapy.crawler import CrawlerProcess
# from scrapy.crawler import CrawlerRunner
# import re
# from scrapy.pipelines.files import FilesPipeline
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
# , Rule

# from  shared_code.pipelines import DownfilesPipeline #(relative)
from azscrapy.items import DownfilesItem
# import os
# from urllib.parse import urlparse
import json

class AnpScrapy(CrawlSpider):
    name = 'AnpScrapy'

    allowed_domains = ['www.gov.br']

    start_urls = [
         'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vcs'
    ]

    folder_name =  "antt"

    def start_requests(self):
        for url in self.start_urls:
            request = scrapy.Request(url, callback=self.parse_link)
            yield request

    def parse_link(self, response):
        links = set( LinkExtractor( allow=('vendas-combustiveis-segmento-m3'), canonicalize=True, unique=True).extract_links(response) )
        for link in links:
            yield scrapy.Request(link.url, callback=self.parse_item, dont_filter=True )

    def parse_item(self, response):
        file_url  = response.css('div#content-core').css("a::attr(href)").get()
        
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        item['original_file_name'] = self.folder_name + '/vendas-combustiveis-segmento-m3.csv'

        yield item
