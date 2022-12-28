import json
import requests
import pandas as pd
import scrapy
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
import logging
from azscrapy.middlewares import AzScrapyCrawlSpiderFiles

class Ibge_PIB(AzScrapyCrawlSpiderFiles):

    logger = logging.getLogger(__name__)

    name            = 'Ibge_PIB'
    allowed_domains = ['servicodados.ibge.gov.br']
    start_urls      = []


    def start_requests(self):

        url             = 'https://servicodados.ibge.gov.br/api/v3/agregados/1621/periodos'
        urlRequest      = requests.get(url).json()
        NomeColunas     = ['id', 'literals', 'modificacao']
        dfPeriodos      = pd.DataFrame(urlRequest, columns = NomeColunas)
        lstPeriodos     = dfPeriodos['id'].to_list()
        lstPeriodos     = [item + '|' for item in lstPeriodos]
        lstPeriodos     = ''.join(lstPeriodos)[:-1]
        self.start_urls = ['https://servicodados.ibge.gov.br/api/v3/agregados/1621/periodos/' + lstPeriodos + '/variaveis/584?localidades=N1[all]&classificacao=11255[90707,93406,93407,93408]']

        for url in self.start_urls:
            request = scrapy.Request(url, callback=self.parse_link)
            yield request


    def parse_link(self, response):

        # folder_name                = "ibge"
        file_url                   = response.url
        item                       = DownfilesItem()
        item['file_urls']          = [file_url]

        item['original_file_name'] = self._FOLDER_NAME + '/' + 'ibge_pib' + '.json'

        yield item