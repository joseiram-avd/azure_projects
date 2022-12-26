import scrapy
import re
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from azscrapy.items import DownfilesItem
import os
from urllib.parse import urlparse
import logging


logger = logging.getLogger(__name__)

class FileException(Exception):
    """General media error exception"""


class Senatran_Frota(CrawlSpider):

    name            = 'Senatran_Frota'
    allowed_domains = ['gov.br']
    start_urls      = ['https://dados.transportes.gov.br/dataset/frota-de-veiculos']
    folder_name     = "senatran"
    meses           = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho',
                        'agosto', 'setembro', 'outubro',' novembro', 'dezembro', 
                        'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 
                        'ago', 'set', 'out',' nov', 'dez']

    
    def mesNum(self,nome_mes):
        if nome_mes == 'Jan':
            mes_num = '01'
        if nome_mes == 'Fev':
            mes_num = '02'
        if nome_mes == 'Mar':
            mes_num = '03'
        if nome_mes == 'Abr':
            mes_num = '04'  
        if nome_mes == 'Mai':
            mes_num = '05'
        if nome_mes == 'Jun':
            mes_num = '06'
        if nome_mes == 'Jul':
            mes_num = '07'
        if nome_mes == 'Ago':
            mes_num = '08'
        if nome_mes == 'Set':
            mes_num = '09'
        if nome_mes == 'Out':
            mes_num = '10'
        if nome_mes == 'Nov':
            mes_num = '11'
        if nome_mes == 'Dez':
            mes_num = '12'

        return(mes_num)



    def start_requests(self):
        for url in self.start_urls:
            try:
                request = scrapy.Request(url, callback=self.parse_link)
            except:
                logger.warning("Erro ao acessar a pagina")
                raise FileException('download-error')              
            yield request
       


    def parse_link(self, response):
        links = set( LinkExtractor( deny=('dicionario'), canonicalize=True, unique=True).extract_links(response) )
        for link in links:
            yield scrapy.Request(link.url, callback=self.parse_item, dont_filter=True )


    def parse_item(self, response):

        file_url = response.css('.resource-url-analytics::attr(href)').get()
        file_url = response.urljoin(file_url)       
        file_extension = file_url.split('.')[-1]
 
        if file_extension not in ('csv'):
            return

        nome_arquivo = file_url.split('/')[-1].replace('-','_')

        if nome_arquivo.replace('.csv','')[-4:].isnumeric():
            nome_arquivo = nome_arquivo.replace('.csv','')[-4:] + '_' + \
                           self.mesNum(nome_arquivo.split('_')[-2][:3].capitalize()) + \
                           '_frota_de_veiculos.csv'
        
        nome_arquivo = nome_arquivo[8:].replace('.csv','') + '_' + nome_arquivo[:7] + '.csv'

        item = DownfilesItem()
        item['file_urls'] = [file_url]
        try:
            item['original_file_name'] = self.folder_name + '/frota_veiculos/' + nome_arquivo
        except:
            logger.warning("Erro ao baixar o arquivo")
            raise FileException('download-error')              
        yield item







