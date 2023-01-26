import scrapy
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
import json
import logging
from azscrapy.middlewares import AzScrapyCrawlSpiderFiles


class BacenScrapy(AzScrapyCrawlSpiderFiles):
    logger = logging.getLogger(__name__)

    name = 'BacenScrapy'

    allowed_domains = ['api.bcb.gov.br']

    start_urls = [
         'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json'
    ]

    codigos_list = [ ['11753', 'taxa_cambio'],
                     ['4390', 'selic'],
                     ['28773', 'empregos_formais_transporte'],
                     ['28553', 'indiceABCR_dessazonalidado']]

    def start_requests(self):
        for tupla in self.codigos_list:

            codigo = tupla[0]
            nome = tupla[1]

            for url in self.start_urls:
                url = url.format( codigo )
                try:
                    request = scrapy.Request(url, callback=self.parse_link)
                except:
                    logger.warning("Erro ao acessar a pagina")
                    raise FileException('download-error')
                request.meta['nome_arquivo'] = nome
                yield request



    def parse_link(self, response):
        nome_arquivo = response.meta['nome_arquivo']
        file_url = response.url
        item = DownfilesItem()
        item['file_urls'] = [file_url]

        item['original_file_name'] = self._FOLDER_NAME + '/' + nome_arquivo + '.json'

        yield item
