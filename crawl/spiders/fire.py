# -*- coding: utf-8 -*-
import string
import urlparse

import scrapy
import logging
#import urlparse

class FireSpider(scrapy.Spider):

    name = "fire"
    allowed_domains = ["www.berliner-feuerwehr.de"]
    start_urls = (
        'http://www.berliner-feuerwehr.de/aktuelles/einsaetze/',
    )
    max_page = None

    def reports(self, response):
        return response.xpath('//div[@class="news-list-item"]/p/span[@class="nlmore"]/a/@href')

    def pages(self, response):
        return response.xpath('//div[@class="news-list-browse"]/ul/li/a/@href')

    """
    def get_pages(self, response):


        pages = [response.urljoin(x.extract()) for x in self.pages(response)]
        pages = sorted(set(pages))
        for page in pages:
            logging.info(page)
        url = response.urljoin(pages[-1].extract())

        yield scrapy.Request(url, callback=self.parse_report_pages)
        for href in self.pages(response):
            url = response.urljoin(href.extract())
            self.article_pages.append(url)
            yield scrapy.Request(url, callback=self.parse_report_pages)
    """

    def parse(self, response):
        reports = self.reports(response)
        for report in reports:
            pass

        pages_links = response.xpath('//div[@class="news-list-browse"]/ul/li/a/@href').extract()
        pages_text = response.xpath('//div[@class="news-list-browse"]/ul/li/a/text()').extract()
        pagedict = dict(zip(pages_text, pages_links))
        logging.info("Current URL: {}".format(response.url))
        #logging.info(pagedict)
        if u'▸' in pagedict:
            logging.info(pages_links)
            url = response.urljoin(pages_links[-2])
            logging.info("next URL: {}".format(url))
            yield scrapy.Request(url, self.parse)
        else:
            self.max_page = str(response.url).split('/')[-2]
            logging.debug("Max page: {}".format(self.max_page))
        #if pages[-1].xpath(u'//a[▸]'):#extract() == u'▸':
        #    url = response.urljoin(pages[-2].xpath('//a/@href').extract())
        #    yield scrapy.Request(url, self.parse)



    def parse_results(self, response):

        logging.info(response.url)
        #reports = self.reports(response)
        #pages = self.pages(response)
        #for s in reports:
        #    logging.info("Einsatz: {}".format(response.urljoin(s.extract())))
        #for s in pages:
        #    logging.info("Seite: {}".format(response.urljoin(s.extract())))

        #pass

    def parse_report_pages(self, response):
        for href in self.pages(response):
            url = response.urljoin(href.extract())
            yield scrapy.Request(url, callback=self.parse_report_pages)

