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


class AnttScrapy(CrawlSpider):
    name = 'AnttScrapy'
    allowed_domains = ['dados.antt.gov.br']
    start_urls = [
      'https://dados.antt.gov.br/dataset/veiculos-habilitados',                              
     'https://dados.antt.gov.br/dataset/licencas-de-viagem-nacional-servico-fretado',       
     'https://dados.antt.gov.br/dataset/motoristas-habilitados'                            
     'https://dados.antt.gov.br/dataset/empresas-habilitadas'                              
     'https://dados.antt.gov.br/dataset/monitriip-servico-regular-viagens',                 
     'https://dados.antt.gov.br/dataset/monitriip-bilhetes-de-passagem',                    
      'https://dados.antt.gov.br/dataset/solicitacoes-de-novos-mercados',                    
     'https://dados.antt.gov.br/dataset/monitriip-servico-fretado-viagens'                  
     ]
   
 
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


    
    def contains_number(self,string):
        return any(char.isdigit() for char in string)
                       

    def start_requests(self):
        for url in self.start_urls:
            try:
                yield scrapy.Request(url, callback=self.parse_link, dont_filter=True)
            except:
                logger.warning("Erro ao acessar a pagina")
                raise FileException('download-error')             
                


    def parse_link(self, response):
        links = set( LinkExtractor( allow=('dataset'), canonicalize=True, unique=True).extract_links(response) )

        for link in links:
            file_url = response.urljoin(link.url.lower())
            file_extension = file_url.split('.')[-1]

            if file_extension not in ('pdf', 'json', 'csv'):
                folder_name = link.text.strip().lower()
                if 'csv' in folder_name:
                    request = scrapy.Request(link.url, callback=self.parse_item, dont_filter=True )
                    request.meta['folder_name'] = folder_name
                    yield request
 
    def parse_item(self, response):

        # defaul value for id seq
        position = '100000'


        title = response.css('title::text').get()
        
        if "Novos Mercados" not in title:
        
            title_split = title .split(' ')
            for item in title_split:
                if self.contains_number(item):
                    data = item
                    if len(data) == 4:
                        data_base = data + '_12'         
                    if len(data) == 5:
                        data_base = '20' + data[3:5] + '_' + self.mesNum(data[0:3])
                    print(data_base)     
        else:

            data_base =  response.css('.resource-url-analytics::attr(href)').get().split('/')[-1][-8:-4] 
        
        

        # looking for position number
        tr_list = response.css('table').css('tr')
        for tr in tr_list:
            th_text = tr.css('th::text').get()
            if th_text == 'position':
                id = tr.css('td::text').get()
                position = str(100000 + int(id))


        # getting the href of file
        # folder_name =  response.meta['folder_name']
        folder_name =  "antt/"
        file_url = response.css('.resource-url-analytics::attr(href)').get()
        file_url = response.urljoin(file_url)
        file_extension = file_url.split('.')[-1]

        if file_extension not in ('csv'):
            return


        if "Novos Mercados" not in title:
            nome_arquivo_link = file_url.split('/')[-1].split('_')
            nome_arquivo = []
            for item in nome_arquivo_link:
                if not self.contains_number(item):
                    nome_arquivo.append(item)
            nome_arquivo = '_'.join(nome_arquivo).replace('.csv','')
        else:
            nome_arquivo = 'solicitacoes_de_mercado'
        

        # item setup
        # if "2022" in file_url:
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        try:
            item['original_file_name'] = folder_name + '/' + nome_arquivo + '/' + position + '_' + nome_arquivo + '_' + data_base + '.csv'
        except:
            logger.warning("Erro ao baixar o arquivo")
            raise FileException('download-error')    
        yield item
