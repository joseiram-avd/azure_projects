# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import os

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter

from azure.synapse.artifacts import ArtifactsClient
from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential

from scrapy.spiders import CrawlSpider

class AzScrapySpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class AzScrapyDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class AzScrapyCrawlSpider(CrawlSpider):
    # This class implement the azure synapse pipeline caller

    # default values from env variables
    _FOLDER_NAME = "scrapy"
    _PIPELINE_NAME = 'PL_Primary_Raw_Scrapy_Log'
    _SCRAPY_ID = 0
    _FN_AZURE_CLIENT_ID = os.getenv("FN_AZURE_CLIENT_ID")
    _FN_AZURE_CLIENT_SECRET = os.getenv("FN_AZURE_CLIENT_SECRET")
    _FN_AZURE_TENANT_ID = os.getenv("FN_AZURE_TENANT_ID")
    _FN_PIPELINE_DEFAULT_NAME = os.getenv("FN_PIPELINE_DEFAULT_NAME")
    _FN_RAW_STORAGE_ACCOUNT_KEY = os.getenv("FN_RAW_STORAGE_ACCOUNT_KEY")
    _FN_RAW_STORAGE_ACCOUNT_NAME = os.getenv("FN_RAW_STORAGE_ACCOUNT_NAME")
    _FN_SYNAPSE_WORKSPACE_NAME = os.getenv("FN_SYNAPSE_WORKSPACE_NAME")
    _SYNAPSE_ENDPOINT = f"https://{_FN_SYNAPSE_WORKSPACE_NAME}.dev.azuresynapse.net"

    # settings para feedexport on azure
    _FN_AZURE_ACCOUT_URL = f"https://{_FN_RAW_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/"
    _FN_AZURE_ACCOUNT_KEY = _FN_RAW_STORAGE_ACCOUNT_KEY

    # synapse client object used to run pipelines
    _CLIENT = None

    # This method connect to the azure
    def connect(self, tenant_id, client_id, client_secret, workspace_name):

        if tenant_id!="" and client_id!="" and client_secret!="":
            # log in using a service principal
            service_principal_credential = ClientSecretCredential(
                tenant_id=tenant_id, # your tenant id
                client_id=client_id, #  your client id
                client_secret=client_secret, # your client secret
            )
            return ArtifactsClient(service_principal_credential, self._SYNAPSE_ENDPOINT)
        else:
            # log in using the credentials on the system (i.e., using the credentials used for configuring the Azure CLI)
            cli_credentials = DefaultAzureCredential()
            return ArtifactsClient(cli_credentials, self._SYNAPSE_ENDPOINT)

    def __init__(self, *args, **kwargs):
        super(AzScrapyCrawlSpider, self).__init__(*args, **kwargs)

        if kwargs.get('foldername'):
            self._FOLDER_NAME = kwargs.get('foldername')

        if kwargs.get('run_after_ingestion'):
            self._PIPELINE_NAME = kwargs.get('run_after_ingestion')

        if kwargs.get('scrapy_id'):
            if int(kwargs.get('scrapy_id')):
                self._SCRAPY_ID = int(kwargs.get('scrapy_id'))


    # connecting to synapse
    def closed( self, reason ):

        if (
                self._FN_AZURE_TENANT_ID
            and self._FN_AZURE_CLIENT_ID
            and self._FN_AZURE_CLIENT_SECRET
            and self._FN_SYNAPSE_WORKSPACE_NAME
            and self._SCRAPY_ID > 0 ):


            self._CLIENT = self.connect(self._FN_AZURE_TENANT_ID, self._FN_AZURE_CLIENT_ID, self._FN_AZURE_CLIENT_SECRET, self._FN_SYNAPSE_WORKSPACE_NAME)

            if self._CLIENT:
                p_pipeline_name = self._PIPELINE_NAME if self._PIPELINE_NAME  else self._FN_PIPELINE_DEFAULT_NAME

                response = self._CLIENT.pipeline.create_pipeline_run(p_pipeline_name, parameters={'id':self._SCRAPY_ID, 'ws_end_point_url':self._SYNAPSE_ENDPOINT})

                run_id = response.run_id

                self.logger.info(run_id)
            else:
                self.logger.error(f"{self.name} was closed. There is no AzureCredential")


class AzScrapyCrawlSpiderFiles(AzScrapyCrawlSpider):

    _FOLDER_NAME = "scrapy"
    _PIPELINE_NAME = None
    _FN_AZURE_CLIENT_ID = os.getenv("FN_AZURE_CLIENT_ID")
    _FN_AZURE_CLIENT_SECRET = os.getenv("FN_AZURE_CLIENT_SECRET")
    _FN_AZURE_TENANT_ID = os.getenv("FN_AZURE_TENANT_ID")
    _FN_PIPELINE_DEFAULT_NAME = os.getenv("FN_PIPELINE_DEFAULT_NAME")
    _FN_RAW_STORAGE_ACCOUNT_KEY = os.getenv("FN_RAW_STORAGE_ACCOUNT_KEY")
    _FN_RAW_STORAGE_ACCOUNT_NAME = os.getenv("FN_RAW_STORAGE_ACCOUNT_NAME")
    _FN_SYNAPSE_WORKSPACE_NAME = os.getenv("FN_SYNAPSE_WORKSPACE_NAME")

    # settings para feedexport on azure
    _FN_AZURE_ACCOUT_URL = f"https://{_FN_RAW_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/"
    _FN_AZURE_ACCOUNT_KEY = _FN_RAW_STORAGE_ACCOUNT_KEY

    custom_settings = {
             'AZURE_ACCESS_KEY': _FN_RAW_STORAGE_ACCOUNT_KEY,
             'AZURE_ACCOUNT_NAME':_FN_RAW_STORAGE_ACCOUNT_NAME,
             'AZURE_ACCOUNT_URL':_FN_AZURE_ACCOUT_URL,
             'AZURE_ACCOUNT_KEY':_FN_AZURE_ACCOUNT_KEY,
    }

    def __init__(self, *args, **kwargs):
        super(AzScrapyCrawlSpiderFiles, self).__init__(*args, **kwargs)


#
# class AzScrapyCrawlSpiderFeedExport(AzScrapyCrawlSpider):
#
#     _FOLDER_NAME = "scrapy"
#     _PIPELINE_NAME = None
#     _FN_AZURE_CLIENT_ID = os.getenv("FN_AZURE_CLIENT_ID")
#     _FN_AZURE_CLIENT_SECRET = os.getenv("FN_AZURE_CLIENT_SECRET")
#     _FN_AZURE_TENANT_ID = os.getenv("FN_AZURE_TENANT_ID")
#     _FN_PIPELINE_DEFAULT_NAME = os.getenv("FN_PIPELINE_DEFAULT_NAME")
#     _FN_RAW_STORAGE_ACCOUNT_KEY = os.getenv("FN_RAW_STORAGE_ACCOUNT_KEY")
#     _FN_RAW_STORAGE_ACCOUNT_NAME = os.getenv("FN_RAW_STORAGE_ACCOUNT_NAME")
#     _FN_SYNAPSE_WORKSPACE_NAME = os.getenv("FN_SYNAPSE_WORKSPACE_NAME")
#
#     # settings para feedexport on azure
#     _FN_AZURE_ACCOUT_URL = f"https://{_FN_RAW_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/"
#     _FN_AZURE_ACCOUNT_KEY = _FN_RAW_STORAGE_ACCOUNT_KEY
#     _FEEDS = {f"azure://{_FN_RAW_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/general/raw/%(name)s/item.json":{"format": "json", "overwrite":"True"}}
#
#     custom_settings = {
#              'AZURE_ACCESS_KEY': _FN_RAW_STORAGE_ACCOUNT_KEY,
#              'AZURE_ACCOUNT_NAME':_FN_RAW_STORAGE_ACCOUNT_NAME,
#              'AZURE_ACCOUNT_URL':_FN_AZURE_ACCOUT_URL,
#              'AZURE_ACCOUNT_KEY':_FN_AZURE_ACCOUNT_KEY,
#              'FEED_STORAGES':{'azure': 'azscrapy.files.AzureFeedStorage'},
#              f'FEEDS':_FEEDS
#     }
#
#     def __init__(self, *args, **kwargs):
#         super(AzScrapyCrawlSpiderFeedExport, self).__init__(*args, **kwargs)
