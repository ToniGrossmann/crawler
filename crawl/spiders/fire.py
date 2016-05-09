# -*- coding: utf-8 -*-
import codecs
import collections
import json
import string
import urlparse

import re
import scrapy
import logging
#import urlparse
import time
from datetime import datetime

import sys

from natsort import natsort_keygen, ns


class FireSpider(scrapy.Spider):

    name = "fire"
    allowed_domains = ["www.berliner-feuerwehr.de"]
    start_urls = (
        'http://www.berliner-feuerwehr.de/aktuelles/einsaetze/',
    )

    def natural_sort(self, l):
        convert = lambda text: int(text) if text.isdigit() else text
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
        return sorted(l, key=alphanum_key)

    def reports(self, response):
        return response.xpath('//div[@class="news-list-item"]/p/span[@class="nlmore"]/a/@href')

    def pages(self, response):
        return response.xpath('//div[@class="news-list-browse"]/ul/li/a/@href')

    def parse(self, response):
        reports = self.reports(response)
        for report in reports:
            url = response.urljoin(report.extract())
            logging.debug("Extracted report URL: {}".format(url))
            yield scrapy.Request(url, self.parse_report_data)
            pass

        pages_links = response.xpath('//div[@class="news-list-browse"]/ul/li/a/@href').extract()
        pages_text = response.xpath('//div[@class="news-list-browse"]/ul/li/a/text()').extract()
        pagedict = dict(zip(pages_text, pages_links))
        logging.info("Current URL: {}".format(response.url))
        #logging.info(pagedict)
        if u'▸' in pagedict:
            logging.info(pages_links)
            first_last = self.natural_sort([item for item, count in collections.Counter(pages_links).items() if count > 1])
            logging.info("first_last: {}".format(first_last))
            if len(first_last) > 1:
                url = response.urljoin(first_last[1])
            else:
                url = response.urljoin(first_last[0])
                #url = response.urljoin(pages_links[-2])
            logging.info("Next URL: {}".format(url))
            yield scrapy.Request(url, self.parse)
        else:
            self.max_page = str(response.url).split('/')[-2]
            logging.debug("Max page: {}".format(self.max_page))
        #if pages[-1].xpath(u'//a[▸]'):#extract() == u'▸':
        #    url = response.urljoin(pages[-2].xpath('//a/@href').extract())
        #    yield scrapy.Request(url, self.parse)

    def parse_report_data(self, response):
        contentdict = {}
        article = response.xpath('//div[@class="news-single-item"]')
        article_content = article.xpath('.//p/text()')
        article_time = datetime.strptime(article.xpath('div[@class="news-list-datetime"]/text()').extract()[0].replace(u'\xa0', u' '), u'%d.%m.%Y   %H:%M')
        title = article.xpath('h1/text()')
        article_start = 2
        if article_content[1].extract() == u' ': article_start = 3

        contentdict['id'] = re.search(r"([0-9]+)\/$", response.url).group(1)
        contentdict['url'] = response.url
        contentdict['title'] = title.extract()[0]
        contentdict['time'] = str(article_time)
        contentdict['address'] = "".join(article_content[0].extract().replace(u'\xa0', u' ').replace(u'\r', u'\n\n').strip())
        contentdict['district'] = "".join(article_content[article_start-1].extract().replace(u'\xa0', u' ').replace(u'\r', u'\n\n').strip())
        contentdict['content'] = "".join(map(lambda x: x.extract().replace(u'\xa0', u' ').replace(u'\r', u'\x1F601').strip().replace(u'\x1F601', u'\n'), article_content[article_start:]))
        reload(sys)
        sys.setdefaultencoding("unicode-escape")
        logging.debug(u''.join(json.dumps(contentdict, indent=True, ensure_ascii=False).replace(u'\\n', u'\n')).decode("unicode-escape"))

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

