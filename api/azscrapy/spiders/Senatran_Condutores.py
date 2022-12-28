import scrapy
import re
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from azscrapy.items import DownfilesItem
import os
from urllib.parse import urlparse
import logging
from azscrapy.middlewares import AzScrapyCrawlSpiderFiles

class Senatran_Condutores(AzScrapyCrawlSpiderFiles):

    logger = logging.getLogger(__name__)

    name            = 'Senatran_Condutores'
    allowed_domains = ['gov.br']
    start_urls      = ['https://www.gov.br/infraestrutura/pt-br/assuntos/transito/conteudo-Senatran/estatisticas-quantidade-de-habilitados-denatran']


    def start_requests(self):
        for url in self.start_urls:
            request = scrapy.Request(url, callback=self.parse_link)
            yield request


    def parse_link(self, response):
        links = set( LinkExtractor( allow=('condutores'), canonicalize=True, unique=True).extract_links(response) )
        for link in links:
            yield scrapy.Request(link.url, callback=self.parse_item, dont_filter=True )


    def parse_item(self, response):

        file_url = response.css('.resource-url-analytics::attr(href)').get()
        file_url = response.urljoin(file_url)
        file_extension = file_url.split('.')[-1]

        if file_extension not in ('csv'):
            return

        nome_arquivo = file_url.split('/')[-1].replace('-','_')

        item = DownfilesItem()
        item['file_urls'] = [file_url]
        try:
            item['original_file_name'] = self._FOLDER_NAME + '/condutores_habilitados/' + nome_arquivo
        except:
            logger.warning("Erro ao baixar o arquivo")
            raise FileException('download-error')
        yield item