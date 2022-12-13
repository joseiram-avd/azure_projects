import scrapy
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
import json

class BacenScrapy(CrawlSpider):
    name = '2BacenScrapy'

    allowed_domains = ['api.bcb.gov.br']

    start_urls = [
         'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json'
    ]

    codigos_list = [ ['11753', 'taxa_cambio'],
                     ['11', 'selic'],                    
                     ['28773', 'empregos_formais_transporte'],
                     ['28553', 'indiceABCR_dessazonalidado']]
                     

    folder_name =  "bacen"

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

