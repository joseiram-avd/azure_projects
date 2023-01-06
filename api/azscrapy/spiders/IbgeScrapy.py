import requests
import pandas as pd
import scrapy
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
import logging
from azscrapy.middlewares import AzScrapyCrawlSpiderFiles

class IbgeScrapy(AzScrapyCrawlSpiderFiles):

    logger = logging.getLogger(__name__)

    name = 'IbgeScrapy'
    allowed_domains = ['www.gov.br']
    start_urls      = []


    def get_IPCA(self):
        url             = 'https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos'
        urlRequest      = requests.get(url).json()
        NomeColunas     = ['id', 'literals', 'modificacao']
        dfPeriodos      = pd.DataFrame(urlRequest, columns = NomeColunas)
        lstPeriodos     = dfPeriodos['id'].to_list()
        lstPeriodos     = [item + '|' for item in lstPeriodos]
        lstPeriodos     = ''.join(lstPeriodos)[:-1]

        return  ['ibge_ipca.json', 'https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/' + lstPeriodos + '/variaveis/63?localidades=N1[all]']

    def get_PIB(self):
        url             = 'https://servicodados.ibge.gov.br/api/v3/agregados/1621/periodos'
        urlRequest      = requests.get(url).json()
        NomeColunas     = ['id', 'literals', 'modificacao']
        dfPeriodos      = pd.DataFrame(urlRequest, columns = NomeColunas)
        lstPeriodos     = dfPeriodos['id'].to_list()
        lstPeriodos     = [item + '|' for item in lstPeriodos]
        lstPeriodos     = ''.join(lstPeriodos)[:-1]

        return  ['ibge_pib.json', 'https://servicodados.ibge.gov.br/api/v3/agregados/1621/periodos/' + lstPeriodos + '/variaveis/584?localidades=N1[all]&classificacao=11255[90707,93406,93407,93408]']

    def get_PMS(self):
        url             = 'https://servicodados.ibge.gov.br/api/v3/agregados/8166/periodos'
        urlRequest      = requests.get(url).json()
        NomeColunas     = ['id', 'literals', 'modificacao']
        dfPeriodos      = pd.DataFrame(urlRequest, columns = NomeColunas)
        lstPeriodos     = dfPeriodos['id'].to_list()
        lstPeriodos     = [item + '|' for item in lstPeriodos]
        lstPeriodos     = ''.join(lstPeriodos)[:-1]

        return  ['ibge_pms.json', 'https://servicodados.ibge.gov.br/api/v3/agregados/8166/periodos/' + lstPeriodos + '/variaveis/11622?localidades=N1[all]&classificacao=11046[all]|12355[all]']

    def get_PNAD(self):
        url             = 'https://servicodados.ibge.gov.br/api/v3/agregados/6381/periodos'
        urlRequest      = requests.get(url).json()
        NomeColunas     = ['id', 'literals', 'modificacao']
        dfPeriodos      = pd.DataFrame(urlRequest, columns = NomeColunas)
        lstPeriodos     = dfPeriodos['id'].to_list()
        lstPeriodos     = [item + '|' for item in lstPeriodos]
        lstPeriodos     = ''.join(lstPeriodos)[:-1]

        return ['ibge_pnad.json', 'https://servicodados.ibge.gov.br/api/v3/agregados/6381/periodos/' + lstPeriodos + '/variaveis/4099?localidades=N1[all]']

    def start_requests(self):
        self.start_urls = [
                             self.get_IPCA(),
                             self.get_PIB(),
                             self.get_PMS(),
                             self.get_PNAD()
                              ]

        for item in self.start_urls:
            file_name = item[0]
            url = item[1]

            request = scrapy.Request(url, callback=self.parse_link)
            request.meta['file_name'] = file_name

            yield request

    def parse_link(self, response):

        file_url  = response.url
        file_name = response.meta['file_name']

        item                       = DownfilesItem()
        item['file_urls']          = [file_url]

        item['original_file_name'] = self._FOLDER_NAME + '/' + file_name

        yield item
