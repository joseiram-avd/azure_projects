
import scrapy 
from scrapy.spiders import Spider
from scrapy.linkextractors import LinkExtractor
from azscrapy.items import DownfilesItem

class AnttSpider(Spider):
    name = 'anttspider'

    allowed_domains = ['dados.antt.gov.br']
    start_urls = ['https://dados.antt.gov.br/dataset/gerenciamento-de-autorizacoes']

    custom_settings = {
        'ITEM_PIPELINES': {
            'azscrapy.pipelines.AzScrapyPipeline':1,
        },
        'FILES_STORE':'C:/web'
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_link, dont_filter=True)

    def parse_link(self, response):
        links = set( LinkExtractor( allow=('dataset'), canonicalize=True, unique=True).extract_links(response) )
        for link in links:
            print( link )
            yield scrapy.Request(link.url, callback=self.parse_item, dont_filter=True)

    def parse_item(self, response):
        file_url = response.css('.resource-url-analytics::attr(href)').get()
        file_url = response.urljoin(file_url)
        file_extension = file_url.split('.')[-1]

        if file_extension not in ('pdf', 'json', 'csv'):
            return

        item = DownfilesItem()
        item['file_urls'] = [file_url]
        item['original_file_name'] = file_url.split('/')[-1]
        if '202022' in item['original_file_name']:
            yield item
