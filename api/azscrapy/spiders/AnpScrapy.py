import scrapy

from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
from azscrapy.items import DownfilesItem
from azscrapy.middlewares import AzScrapyCrawlSpiderFiles

class AnpScrapy(AzScrapyCrawlSpiderFiles):
    name = 'AnpScrapy'
    allowed_domains = ['www.gov.br']

    start_urls = [
         'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vcs'
    ]

    def start_requests(self):
        for url in self.start_urls:
            request = scrapy.Request(url, callback=self.parse_link)
            yield request

    def parse_link(self, response):
        links = set( LinkExtractor( allow=('vendas-combustiveis-segmento-m3'), canonicalize=True, unique=True).extract_links(response) )
        for link in links:
            yield scrapy.Request(link.url, callback=self.parse_item, dont_filter=True )

    def parse_item(self, response):
        file_url  = response.css('div#content-core').css("a::attr(href)").get()
        item = DownfilesItem()
        item['file_urls'] = [file_url]
        item['original_file_name'] = self._FOLDER_NAME + '/vendas-combustiveis-segmento-m3.csv'
         
        yield item
