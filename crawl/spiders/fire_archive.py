# -*- coding: utf-8 -*-
import codecs
import collections
import json
import os
import string
import urlparse

import re
import scrapy
import logging
import sys
# import urlparse
import time
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing.schema import Column
from sqlalchemy import Column, Integer, String, Boolean, create_engine
from sqlalchemy import func
from crawl.spiders.fire import FireSpider

class FireArchiveSpider(FireSpider):
    name = "fire_archive"
    allowed_domains = ["www.berliner-feuerwehr.de"]
    start_urls = (
        'http://www.berliner-feuerwehr.de/aktuelles/einsatzarchiv/',
    )

    def __init__(self):
        self.parse = self.parse_monthly_reports

    def archive_months(self, response):
        return response.xpath('//div[@class="news-amenu-container"]/ul/li/a/@href')

    def parse_monthly_reports(self, response):
        archive_months = self.archive_months(response)
        for month in archive_months:
            url = response.urljoin(month.extract())
            yield scrapy.Request(url, self.parse_reports)
"""
    def parse_monthly_reports(self, response):
        session = self.Session()
        reports = self.reports(response)
        # contentdict['id'] = re.search(r"([0-9]+)\/$", response.url).group(1)

        for report in reports:
            report_id = re.search(r"([0-9]+)\/$", report.extract()).group(1)

            if session.query(func.count(Report.id)).filter_by(id=int(report_id)).first()[0] == 0:
                url = response.urljoin(report.extract())
                logging.debug("Extracted report URL: {}".format(url))
                yield scrapy.Request(url, self.parse_report_data)

        pages_links = response.xpath('//div[@class="news-list-browse"]/ul/li/a/@href').extract()
        pages_text = response.xpath('//div[@class="news-list-browse"]/ul/li/a/text()').extract()
        pagedict = dict(zip(pages_text, pages_links))
        logging.info("Current URL: {}".format(response.url))
        # logging.info(pagedict)
        if u'▸' in pagedict:
            logging.debug(pages_links)
            first_last = self.natural_sort(
                [item for item, count in collections.Counter(pages_links).items() if count > 1])
            logging.info("first_last: {}".format(first_last))
            if len(first_last) > 1:
                url = response.urljoin(first_last[1])
            else:
                url = response.urljoin(first_last[0])
                # url = response.urljoin(pages_links[-2])
            logging.info("Next URL: {}".format(url))
            yield scrapy.Request(url, self.parse_monthly_reports)
        else:
            self.max_page = str(response.url).split('/')[-2]
            logging.debug("Max page: {}".format(self.max_page))
            # if pages[-1].xpath(u'//a[▸]'):#extract() == u'▸':
            #    url = response.urljoin(pages[-2].xpath('//a/@href').extract())
            #    yield scrapy.Request(url, self.parse)""""""

    def parse_report_data(self, response):
        session = self.Session()
        contentdict = {}
        article = response.xpath('//div[@class="news-single-item"]')
        article_content = article.xpath('.//p/text()')
        article_content_html = article.xpath('.//p')
        article_time = datetime.strptime(
            article.xpath('div[@class="news-list-datetime"]/text()').extract()[0].replace(u'\xa0', u' '),
            u'%d.%m.%Y   %H:%M')
        title = article.xpath('h1/text()')
        article_start = 2
        if article_content[1].extract() == u' ': article_start = 3
        content_text = u''
        contentdict['id'] = int(re.search(r"([0-9]+)\/$", response.url).group(1))
        #if contentdict['id'] == 3002: return
        contentdict['url'] = response.url
        contentdict['title'] = title.extract()[0]
        contentdict['time'] = str(article_time)
        contentdict['street'] = "".join(
            article_content[0].extract().replace(u'\xa0', u' ').strip())

        # treatment of special case when street is in a font-tag
        if not contentdict['street']:
            if len(article.xpath('.//p/font/text()')) > 0:
                contentdict['street'] = "".join(
                article.xpath('.//p/font/text()')[0].extract().replace(u'\xa0', u' ').strip())

        contentdict['street'] = re.sub(r'^: ', '', contentdict['street'])


        contentdict['district'] = "".join(
            article_content[article_start - 1].extract().replace(u'\xa0', u' ').strip())
        contentdict['district'] = re.sub(r'^: ', '', contentdict['district'])
        # move street and/or district inside content if a certain length is exceeded
        if len(contentdict['street']) > 100:
            #content_text += ''.join(contentdict['street'] + '\r')
            contentdict['street'] = ''
            article_start -= 1
        if len(contentdict['district']) > 30:
            # content_text += ''.join(contentdict['district'] + '\r')
            contentdict['district'] = ''
            article_start -= 1

        # content_text += ''.join(map(lambda x: x.extract().replace(u'\xa0', u' ').replace('\r', u'\x1F601').strip().replace(u'\x1F601', '\n'), article_content[article_start:]))
        content_text += ''.join(map(lambda x: x.extract().replace('\r', u'\x1F601').strip().replace(u'\x1F601', '\n'), article_content_html[1:]))

        # treatment of special case when content contains words at the start that indicate a location
        if content_text.startswith(("Bezirk:", "Ort:", "Ortsteil:", "Ortsteil :")):
            contentdict['district'] = re.sub(r'(Ort:|Bezirk:|Ortsteil:|Ortsteil :)\s*', '', content_text.splitlines()[0]).strip()
            # removes first line of the string
            content_text = '\n'.join(content_text.split('\n')[1:])

        # treatment of special case when street and district is missing
        #if not contentdict['street'] and not contentdict['district']:
        #    content_text = ''.join(article.xpath('.//p').extract())
        #    content_text = content_text.replace('\r', u'\x1F601').strip().replace(u'\x1F601', '\n')

        # remove HTML and replace CR by LF
        content_text = re.sub('<[^<]+?>', '', content_text).replace(u'\r', u'\x0a')
        contentdict['content'] = content_text


        session.add(Report(id = contentdict['id'], street = contentdict['street'],
                           content = contentdict['content'], district = contentdict['district'], url = contentdict['url'], time = contentdict['time'],
                           title = contentdict['title']))
        session.commit()

        reload(sys)
        sys.setdefaultencoding("unicode-escape")
        logging.debug(u''.join(json.dumps(contentdict, indent=True, ensure_ascii=False).replace(u'\\n', u'\n')))
"""