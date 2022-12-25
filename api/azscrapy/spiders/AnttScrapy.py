import scrapy

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
from azscrapy.middlewares import CrawlSpiderMiddleware

class AnttScrapy(CrawlSpiderMiddleware):
    name = 'AnttScrapy'
    allowed_domains = ['dados.antt.gov.br']

    start_urls = [
    'https://dados.antt.gov.br/dataset/veiculos-habilitados',
    'https://dados.antt.gov.br/dataset/gerenciamento-de-autorizacoes' ,
    'https://dados.antt.gov.br/dataset/licencas-operacionais',
    'https://dados.antt.gov.br/dataset/licencas-de-viagem-nacional-servico-fretado',
    'https://dados.antt.gov.br/dataset/licencas-de-viagem-internacional-servico-fretado',
    'https://dados.antt.gov.br/dataset/motoristas-habilitados',
    'https://dados.antt.gov.br/dataset/autorizacoes-de-viagem-internacional-servico-fretado-continuo',
    'https://dados.antt.gov.br/dataset/empresas-habilitadas',
    'https://dados.antt.gov.br/dataset/monitriip-servico-fretado-paradas',
    'https://dados.antt.gov.br/dataset/monitriip-servico-regular-paradas',
    'https://dados.antt.gov.br/dataset/monitriip-servico-fretado-viagens',
    'https://dados.antt.gov.br/dataset/monitriip-servico-regular-viagens',
    'https://dados.antt.gov.br/dataset/monitriip-bilhetes-de-passagem',
    'https://dados.antt.gov.br/dataset/solicitacoes-de-novos-mercados',
    ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_link, dont_filter=True)

    def parse_link(self, response):
        links = set( LinkExtractor( allow=('dataset'), canonicalize=True, unique=True).extract_links(response) )

        for link in links:
            file_url = response.urljoin(link.url.lower())
            file_extension = file_url.split('.')[-1]

            if file_extension not in ('csv'):
                folder_name = link.text.strip().lower()
                if 'csv' in folder_name:
                    request = scrapy.Request(link.url, callback=self.parse_item, dont_filter=True )

                    yield request

    def parse_item(self, response):
        # defaul value for id seq
        position = '100000'

        # looking for position number
        tr_list = response.css('table').css('tr')
        for tr in tr_list:
            th_text = tr.css('th::text').get()
            if th_text == 'position':
                id = tr.css('td::text').get()
                position = str(100000 + int(id))

        file_url = response.css('.resource-url-analytics::attr(href)').get()
        file_url = response.urljoin(file_url)
        file_extension = file_url.split('.')[-1]

        if file_extension not in ('csv'):
            return

        # item
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        item['original_file_name'] = self._FOLDER_NAME + '/' + position + '_' + file_url.split('/')[-1]

        yield item
