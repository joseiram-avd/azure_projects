import requests
import pandas as pd
import scrapy
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
import logging

logger = logging.getLogger(__name__)


class FileException(Exception):
    """General media error exception"""

class Ibge_IPCA(CrawlSpider):
    name = 'Ibge_IPCA'
    allowed_domains = ['www.gov.br']
    start_urls      = []

    def start_requests(self):
    
        url             = 'https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos'
        urlRequest      = requests.get(url).json()
        NomeColunas     = ['id', 'literals', 'modificacao']
        dfPeriodos      = pd.DataFrame(urlRequest, columns = NomeColunas)
        lstPeriodos     = dfPeriodos['id'].to_list()
        lstPeriodos     = [item + '|' for item in lstPeriodos] 
        lstPeriodos     = ''.join(lstPeriodos)[:-1]
        self.start_urls = ['https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/' + lstPeriodos + '/variaveis/63?localidades=N1[all]']


        for url in self.start_urls:
            try:
                request = scrapy.Request(url, callback=self.parse_link)
            except:
                logger.warning("Erro ao acessar a pagina")
                raise FileException('download-error')     
            yield request
                       

    def parse_link(self, response):
        
        folder_name                = "ibge"
        file_url                   = response.url
        item                       = DownfilesItem()
        item['file_urls']          = [file_url]
        try:
            item['original_file_name'] = folder_name + '/' + 'ibge_ipca' + '.json'
        except:
            logger.warning("Erro ao baixar o arquivo")
            raise FileException('download-error')    
        yield item



  
