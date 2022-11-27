import scrapy


class DownfilesItem(scrapy.Item):
	# define the fields for your item here like:
	file_urls = scrapy.Field()
	original_file_name = scrapy.Field()
	files = scrapy.Field()