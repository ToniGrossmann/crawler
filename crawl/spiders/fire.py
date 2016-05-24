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
# import urlparse
import time
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing.schema import Column
from sqlalchemy import Column, Integer, String, Boolean, create_engine
from sqlalchemy import func
import sys

import sqlite3
from natsort import natsort_keygen, ns

Base = declarative_base()


class FireSpider(scrapy.Spider):
    name = "fire"
    allowed_domains = ["www.berliner-feuerwehr.de"]
    start_urls = (
        'http://www.berliner-feuerwehr.de/aktuelles/einsaetze/',
    )
    conn = None

    def __init__(self):

        engine = create_engine('sqlite:///sqlite.db', echo=False)
        self.Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.parse = self.parse_reports

    def natural_sort(self, l):
        convert = lambda text: int(text) if text.isdigit() else text
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
        return sorted(l, key=alphanum_key)

    def reports(self, response):
        return response.xpath('//div[@class="news-list-item"]/p/span[@class="nlmore"]/a/@href')

    def pages(self, response):
        return response.xpath('//div[@class="news-list-browse"]/ul/li/a/@href')

    #def parse(self, response):
    #    pass

    def parse_reports(self, response):
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
            logging.info("Next URL: {}".format(url))
            yield scrapy.Request(url, self.parse_reports)
        else:
            self.max_page = str(response.url).split('/')[-2]
            logging.debug("Max page: {}".format(self.max_page))

    def parse_report_data(self, response):
        session = self.Session()
        contentdict = {}
        article = response.xpath('//div[@class="news-single-item"]')
        article_content = article.xpath('.//*/p/text()')
        article_content_html = article.xpath('.//*/p')
        article_time = datetime.strptime(
            article.xpath('div[@class="news-list-datetime"]/text()').extract()[0].replace(u'\xa0', u' '),
            u'%d.%m.%Y   %H:%M')
        title = article.xpath('h1/text()')
        article_start = 0
        contentdict['district'] = ''
        contentdict['street'] = ''
        if len(article_content) > 1:
            if len(article_content[0].extract()) < 70:
                contentdict['street'] = re.sub('^[ \:]*[^a-zA-Z]', '', article_content[0].extract()).strip()
                article_start += 1
            if len(article_content[1].extract()) < 50 and len(article_content) > 2:
                contentdict['district'] = re.sub('^[ \:]*[^a-zA-Z]', '', article_content[1].extract()).strip()
                if len(contentdict['district']) == 0 and len(article_content[2].extract()) < 50: contentdict['district'] = re.sub('^[ \:]*[^a-zA-Z]', '', article_content[2].extract()).strip()
                article_start += 1

        #if len(article_content) > 1 and re.match(r'(Straße|Bezirk|Ort|Adresse)', article_content[0].extract()):#article_content[0].extract().startswith(("<b>Bezirk", "<b>Ort", "<b>Ortsteil", "<b>Adresse")):
        #    article_start = 0
        #elif len(article_content) > 1:
        #    article_start = 1
        #elif len(article_content) > 1 and article_content[1].extract() == u' ': article_start = 2
        content_text = u''
        contentdict['id'] = int(re.search(r"([0-9]+)\/$", response.url).group(1))
        #if contentdict['id'] == 3002: return
        contentdict['url'] = response.url
        contentdict['title'] = title.extract()[0]
        contentdict['time'] = str(article_time)

        #if len(article_content) > 1:
        #    for i in article_content:
        #        if re.match(r'(Adresse|Straße)', i.extract()): contentdict['street'] = i.extract().replace(u'\xa0', u' ').strip()
        #        if re.match(r'Ort', i.extract()): contentdict['district'] = i.extract().replace(u'\xa0', u' ').strip()
        """
        contentdict['street'] = "".join(
            article_content[0].extract().replace(u'\xa0', u' ').strip())

        # treatment of special case when street is in a font-tag
        if not contentdict['street']:
            if len(article.xpath('.//p/font/text()')) > 0:
                contentdict['street'] = "".join(
                article.xpath('.//p/font/text()')[0].extract().replace(u'\xa0', u' ').strip())

        contentdict['street'] = re.sub(r'^:( *)', '', contentdict['street'])
        contentdict['district'] = "".join(
            article_content[article_start].extract().replace(u'\xa0', u' ').strip())
        contentdict['district'] = re.sub(r'^:( *)', '', contentdict['district'])
        # move street and/or district inside content if a certain length is exceeded
        if len(contentdict['street']) > 100:
            contentdict['street'] = ''
            if article_start > 0: article_start -= 1
        if len(contentdict['district']) > 30:
            contentdict['district'] = ''
            if article_start > 0: article_start -= 1
        """
        logging.info('article_start: {}, url: {}'.format(article_start, response.url))
        # content_text += ''.join(map(lambda x: x.extract().replace(u'\xa0', u' ').replace('\r', u'\x1F601').strip().replace(u'\x1F601', '\n'), article_content[article_start:]))
        #if len(article_content_html) < 3: article_start = 0

        article_start = 0 #workaround for reports having address and district inside the same tag as the content
        content_text += ''.join(map(lambda x: x.extract().replace('\r', u'\x1F601').strip().replace(u'\x1F601', '\n'), article_content_html[(lambda x: x-1 if x > 0 else 0)(article_start):]))
        # remove HTML and replace CR by LF
        content_text = re.sub('<[^<]+?>', '', content_text).replace(u'\r', u'\x0a')

        # treatment of special case when content contains words at the start that indicate a location

        #if re.match(r'^(Bezirk|Ort)', content_text):#content_text.startswith(("Bezirk:", "Ort:", "Ortsteil:", "Ortsteil :")):
            #contentdict['district'] = re.sub(r'(Ort|Bezirk|Ortsteil|Ortsteil)( *):\s*', '', content_text.splitlines()[0]).strip()
            # removes first line of the string
        #    content_text = '\n'.join(content_text.split('\n')[1:]).strip()
        begin_index = 0
        #print content_text
        for i, val in enumerate(content_text.split('\n')):
            if len(val) > 70:
                begin_index = i
                break
        #print '###'
        #temp = content_text.split('\n')[begin_index:]
        #print '\n'.join(temp)
        contentdict['content'] = '\n'.join(content_text.split('\n')[begin_index:]).strip()#content_text[begin_index:]

        if contentdict['content'].startswith(("Adresse", "Ort")):
            if len(contentdict['content'].split('\n')[0]) < 100:
                #print("specialcase - " + str(contentdict['id']) + " :\n" + contentdict['content'].split('\n')[0])
                contentdict['content'] = '\n'.join(contentdict['content'].split('\n')[1:]).strip()

        session.add(Report(id = contentdict['id'], street = contentdict['street'],
                           content = contentdict['content'], district = contentdict['district'], url = contentdict['url'], time = contentdict['time'],
                           title = contentdict['title']))
        session.commit()

        reload(sys)
        sys.setdefaultencoding("unicode-escape")
        logging.debug(u''.join(json.dumps(contentdict, indent=True, ensure_ascii=False).replace(u'\\n', u'\n')))

class Report(Base):
    __tablename__ = 'reports'
    id = Column(Integer, primary_key=True)
    street = Column(String)
    content = Column(String)
    district = Column(String)
    url = Column(String)
    time = Column(String)
    title = Column(String)
    kind = Column(String, default='fire')