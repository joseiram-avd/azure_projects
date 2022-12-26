import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
import json
import logging
import requests
import urllib.request
logger = logging.getLogger(__name__)


class FileException(Exception):
    """General media error exception"""


class AnpScrapy(CrawlSpider):
    name = 'AnpScrapy'

    allowed_domains = ['www.gov.br']

    start_urls = [
         'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vcs'
    ]

    folder_name =  "anp"


    def start_requests(self):
        for url in self.start_urls:
            try:
                request = scrapy.Request(url, callback=self.parse_link)
            except:
                logger.warning("Erro ao acessar a pagina")
                raise FileException('download-error')              
            yield request
                     

    def parse_link(self, response):
        links = set( LinkExtractor( allow=('vendas-combustiveis-segmento-m3'), canonicalize=True, unique=True).extract_links(response) )
        for link in links:
            yield scrapy.Request(link.url, callback=self.parse_item, dont_filter=True )

    def parse_item(self, response):
        file_url  = response.css('div#content-core').css("a::attr(href)").get()
        
        item = DownfilesItem()
        item['file_urls'] = [file_url]

        try:
            item['original_file_name'] = self.folder_name + '/vendas-combustiveis-segmento-m3.csv'
        except:
            logger.warning("Erro ao baixar o arquivo")
            raise FileException('download-error')              
        yield item


    def closed( self, reason ):
        adf_api_version = '/createRun?api-version=2020-12-01'
        adf_url = 'https://syn-us2-demo-jias.dev.azuresynapse.net/pipelines/'
        adf_pipeline =  'PL_Run_Wait'
        url = adf_url + adf_pipeline + adf_api_version
        print ( url )
        requisicao = urllib.request.urlopen(url).read()