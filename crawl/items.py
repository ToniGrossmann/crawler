# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    kind = 'FIRE'
    street = scrapy.Field()
    district = scrapy.Field()
    time = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()


