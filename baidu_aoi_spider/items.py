# items.py
import scrapy

class BaiduAoiItem(scrapy.Item):
    idx = scrapy.Field()
    uid_name = scrapy.Field()
    rank = scrapy.Field()
    geometry = scrapy.Field()
    status = scrapy.Field()