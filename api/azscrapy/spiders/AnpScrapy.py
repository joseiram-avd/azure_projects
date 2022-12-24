import scrapy
import urllib
import logging
import os

# import wget
# import scrapy
# from bs4 import BeautifulSoup
# import azure.functions as func
# from scrapy.crawler import CrawlerProcess
# from scrapy.crawler import CrawlerRunner
# import re
# from scrapy.pipelines.files import FilesPipeline
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
# , Rule
# from  shared_code.pipelines import DownfilesPipeline #(relative)
from azscrapy.items import DownfilesItem
# import os
# from urllib.parse import urlparse
# import json
# from scrapy import signals

from azure.synapse.artifacts import ArtifactsClient
from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential

class AnpScrapy(CrawlSpider):
    name = 'AnpScrapy'
    allowed_domains = ['www.gov.br']

    start_urls = [
         'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vcs'
    ]

    # the name of the workspace
    workspace_name=os.getenv("SYNAPSE_WORKSPACE")

    # the three parameters obtained after creating the service principal
    tenant_id=os.getenv("AZURE_TENANT_ID") #"167e5ca3-1716-4d6a-9173-e3703dc69265" # your tenant id
    client_id=os.getenv("AZURE_CLIENT_ID") #"9f145146-1539-454c-b1ed-aae3767d787f" #  your Application (client) ID
    client_secret=os.getenv("AZURE_CLIENT_SECRET") #"trj8Q~aUvwkRMHAVPHl68iB6c8dTmFA_hg1yKa3~" # the Application secret_value

    # default values for missing scrapy parameters 
    folder_name =  "scrapy"

    # the name of the pipeline to be triggered (P.S.: this pipeline should be a test pipeline that doesn't incur any costs)
    pipeline_name="PL_Run_Wait"
    
    # the api caller client
    client=None

    # the specific pipeline to run from control table 
    run_after_ingestion=pipeline_name

    def connect(self, tenant_id, client_id, client_secret, workspace_name):
        if tenant_id!="" and client_id!="" and client_secret!="":
            # log in using a service principal
            service_principal_credential = ClientSecretCredential(
                tenant_id=tenant_id, # your tenant id
                client_id=client_id, #  your client id
                client_secret=client_secret, # your client secret
            )
            return ArtifactsClient(service_principal_credential,f"https://{workspace_name}.dev.azuresynapse.net")
        else:
            # log in using the credentials on the system (i.e., using the credentials used for configuring the Azure CLI)
            cli_credentials = DefaultAzureCredential()
            return ArtifactsClient(cli_credentials,f"https://{workspace_name}.dev.azuresynapse.net")

    def closed( self, reason ):
        if self.client:
            p_pipeline_name = self.run_after_ingestion if self.run_after_ingestion  else self.pipeline_name
            response = self.client.pipeline.create_pipeline_run(p_pipeline_name)
            run_id = response.run_id

            self.logger.info(run_id)
        else:
            self.logger.error(f"{self.name} was closed. There is no AzureCredential")
             

    def __init__(self, *args, **kwargs):
        super(AnpScrapy, self).__init__(*args, **kwargs)

        self.folder_name = kwargs.get('foldername').lower()
        self.run_after_ingestion =  kwargs.get('run_after_ingestion') 
        
        self.client = self.connect(self.tenant_id, self.client_id, self.client_secret, self.workspace_name)


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
        item['original_file_name'] = self.folder_name + '/vendas-combustiveis-segmento-m3.csv'

        yield item
