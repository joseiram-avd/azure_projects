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
def run_spider(spider_name, foldername, run_after_ingestion):
        m = __import__(f"azscrapy.spiders.{spider_name}" )
        settings = get_project_settings()
        crawler = CrawlerRunner(settings)
        crawler.crawl( eval("azscrapy.spiders.{}.{}".format(spider_name, spider_name)), foldername=foldername.lower(), run_after_ingestion=run_after_ingestion )

def main(req: func.HttpRequest) -> func.HttpResponse:
    # logging.info('Python HTTP trigger function processed a request.')
    name = req.params.get('name')
    foldername = req.params.get('foldername')
    run_after_ingestion = req.params.get('run_after_ingestion')

    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
            foldername = req_body.get('foldername')
            run_after_ingestion = req_body.get('run_after_ingestion')
          
    if name:
        run_spider(name, foldername, run_after_ingestion)
        return func.HttpResponse(f"{name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully, but no spiders were executed.",
            status_code=200
        )


