# -*- coding: utf-8 -*-
import scrapy
import logging
#import urlparse

class FireSpider(scrapy.Spider):
    name = "fire"
    allowed_domains = ["http://www.berliner-feuerwehr.de/aktuelles/einsaetze/"]
    start_urls = (
        'http://www.berliner-feuerwehr.de/aktuelles/einsaetze/',
    )

    def parse(self, response):
        selectors = response.xpath('//div[@class="news-list-item"]/p/span[@class="nlmore"]/a/@href')
        logging.info(selectors)
        for s in selectors:
            logging.info(response.urljoin(s.extract()))
        pass
