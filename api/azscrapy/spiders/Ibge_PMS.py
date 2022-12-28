import requests
import pandas as pd
import scrapy
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
import logging
from azscrapy.middlewares import AzScrapyCrawlSpiderFiles


class Ibge_PMS(AzScrapyCrawlSpiderFiles):
    logger = logging.getLogger(__name__)

    name = 'Ibge_PMS'
    allowed_domains = ['www.gov.br']
    start_urls      = []

    def start_requests(self):

        url             = 'https://servicodados.ibge.gov.br/api/v3/agregados/8166/periodos'
        urlRequest      = requests.get(url).json()
        NomeColunas     = ['id', 'literals', 'modificacao']
        dfPeriodos      = pd.DataFrame(urlRequest, columns = NomeColunas)
        lstPeriodos     = dfPeriodos['id'].to_list()
        lstPeriodos     = [item + '|' for item in lstPeriodos]
        lstPeriodos     = ''.join(lstPeriodos)[:-1]
        self.start_urls = ['https://servicodados.ibge.gov.br/api/v3/agregados/8166/periodos/' + lstPeriodos + '/variaveis/11622?localidades=N1[all]&classificacao=11046[all]|12355[all]']

        for url in self.start_urls:
            request = scrapy.Request(url, callback=self.parse_link)
            yield request

    def parse_link(self, response):
        # folder_name                = "ibge"
        file_url                   = response.url
        item                       = DownfilesItem()
        item['file_urls']          = [file_url]

        item['original_file_name'] = self._FOLDER_NAME + '/' + 'ibge_pms' + '.json'

        yield item