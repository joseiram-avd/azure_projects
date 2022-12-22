import scrapy
import urllib

import logging
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

    # default values for missing scrapy parameters 
    folder_name =  "scrapy"
    adf_pipeline_after_close = 'PL_Run_Wait'


    def __init__(self, *args, **kwargs):
        super(AnpScrapy, self).__init__(*args, **kwargs)
        self.folder_name = kwargs.get('foldername').lower()
        self.adf_pipeline_after_close = kwargs.get('run_after_ingestion')

    def closed( self, reason ):
        adf_api_version = '/createRun?api-version=2020-12-01'
        adf_url = 'https://syn-us2-demo-jias.dev.azuresynapse.net/pipelines/'
        adf_pipeline =  self.adf_pipeline_after_close
        url = adf_url + adf_pipeline + adf_api_version

        print ( url )

        requisicao = urllib.request.urlopen(url).read()
        

    def start_requests(self):
        adf_api_version = '/createRun?api-version=2020-12-01'
        adf_url = 'https://syn-us2-demo-jias.dev.azuresynapse.net/pipelines/'
        adf_pipeline =  self.adf_pipeline_after_close
        url = adf_url + adf_pipeline + adf_api_version

        print ( url )

        requisicao = urllib.request.urlopen(url).read()
        
        for url in self.start_urls:
            print ( url ) 
            request = scrapy.Request(url, callback=self.parse_link)
            yield request

    def parse_link(self, response):
        links = set( LinkExtractor( allow=('vendas-combustiveis-segmento-m3'), canonicalize=True, unique=True).extract_links(response) )
        for link in links:
            print ( link ) 
            yield scrapy.Request(link.url, callback=self.parse_item, dont_filter=True )

    def parse_item(self, response):
        file_url  = response.css('div#content-core').css("a::attr(href)").get()
        
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        item['original_file_name'] = self.folder_name + '/vendas-combustiveis-segmento-m3.csv'

        yield item
