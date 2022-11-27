# import logging
# import scrapy
import azure.functions as func
from scrapy.crawler import CrawlerRunner
# import re
# from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
# from urllib.parse import urlparse

# Reactor restart
from crochet import setup, wait_for
# import importlib
import azscrapy.spiders

setup()

# @wait_for(10)
def run_spider(spider_name):
        m = __import__(f"azscrapy.spiders.{spider_name}" )

        settings = get_project_settings()
        crawler = CrawlerRunner( settings )
        crawler.crawl( eval("azscrapy.spiders.{}.{}".format(spider_name, spider_name)) )

def main(req: func.HttpRequest) -> func.HttpResponse:
    # logging.info('Python HTTP trigger function processed a request.')
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        run_spider(name)
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully, but no spiders were executed.",
            status_code=200
        )


