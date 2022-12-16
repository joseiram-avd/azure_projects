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

class BacenScrapy(CrawlSpider):
    name = 'BacenScrapy'

    allowed_domains = ['api.bcb.gov.br']

    start_urls = [
         'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json'
    ]

    codigos_list = [ ['11753', 'taxa_cambio'],
                     ['11', 'selic'],                    
                     ['28773', 'empregos_formais_transporte'],
                     ['28553', 'indiceABCR_dessazonalidado']]
                     

    folder_name =  "scrapy"

    def __init__(self, *args, **kwargs):
        super(BacenScrapy, self).__init__(*args, **kwargs)
        self.folder_name = kwargs.get('foldername').lower()

    def start_requests(self):
        for tupla in self.codigos_list:

            codigo = tupla[0]
            nome = tupla[1]

            for url in self.start_urls:
                url = url.format( codigo )
                request = scrapy.Request(url, callback=self.parse_link)
                request.meta['nome_arquivo'] = nome
                yield request

    def parse_link(self, response):
        nome_arquivo = response.meta['nome_arquivo']
        file_url = response.url
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        item['original_file_name'] = self.folder_name + '/' + nome_arquivo + '.json'

        yield item

