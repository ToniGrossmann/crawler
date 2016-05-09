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

import sys

import sqlite3
from natsort import natsort_keygen, ns


class FireSpider(scrapy.Spider):
    name = "fire"
    allowed_domains = ["www.berliner-feuerwehr.de"]
    start_urls = (
        'http://www.berliner-feuerwehr.de/aktuelles/einsaetze/',
    )
    conn = None

    def __init__(self):
        self.connect_to_sqlite()

    def connect_to_sqlite(self):
        dbfile = u'sqlite3.db'
        d = os.path.dirname(os.path.abspath(dbfile))
        if not os.path.exists(d):
            os.makedirs(d)
        if os.path.isfile(dbfile):
            self.conn = sqlite3.connect(dbfile)
        else:
            self.conn = sqlite3.connect(dbfile)
            c = self.conn.cursor()
            c.execute(
                '''CREATE TABLE reports (id INTEGER PRIMARY KEY, address TEXT, content TEXT, district TEXT, url TEXT, time TEXT, title TEXT)''')
            self.conn.commit()

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
        # contentdict['id'] = re.search(r"([0-9]+)\/$", response.url).group(1)

        for report in reports:
            report_id = (re.search(r"([0-9]+)\/$", report.extract()).group(1),)
            check_id = \
                list(
                    self.conn.cursor().execute('''SELECT count(id) FROM reports WHERE id = ?''', report_id).fetchone())[
                    0]
            if check_id == 0:
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
            yield scrapy.Request(url, self.parse)
        else:
            self.max_page = str(response.url).split('/')[-2]
            logging.debug("Max page: {}".format(self.max_page))
            # if pages[-1].xpath(u'//a[▸]'):#extract() == u'▸':
            #    url = response.urljoin(pages[-2].xpath('//a/@href').extract())
            #    yield scrapy.Request(url, self.parse)

    def parse_report_data(self, response):
        contentdict = {}
        article = response.xpath('//div[@class="news-single-item"]')
        article_content = article.xpath('.//p/text()')
        article_time = datetime.strptime(
            article.xpath('div[@class="news-list-datetime"]/text()').extract()[0].replace(u'\xa0', u' '),
            u'%d.%m.%Y   %H:%M')
        title = article.xpath('h1/text()')
        article_start = 2
        if article_content[1].extract() == u' ': article_start = 3
        content_text = u''
        contentdict['id'] = re.search(r"([0-9]+)\/$", response.url).group(1)
        if contentdict['id'] == '3002': return
        contentdict['url'] = response.url
        contentdict['title'] = title.extract()[0]
        contentdict['time'] = str(article_time)
        contentdict['address'] = "".join(
            article_content[0].extract().replace(u'\xa0', u' ').replace(u'\r', u'\n\n').strip())
        contentdict['address'] = re.sub(r'^: ', '', contentdict['address'])
        contentdict['district'] = "".join(
            article_content[article_start - 1].extract().replace(u'\xa0', u' ').replace(u'\r', u'\n\n').strip())
        contentdict['district'] = re.sub(r'^: ', '', contentdict['district'])
        # move address and/or district inside content if a certain length is exceeded
        if len(contentdict['address']) > 40:
            content_text += ''.join(contentdict['address'] + '\r')
            contentdict['address'] = ''
            article_start -= 1
        if len(contentdict['district']) > 25:
            content_text += ''.join(contentdict['district'] + '\r')
            contentdict['district'] = ''
            article_start -= 1

        # content_text += ''.join(map(lambda x: x.extract().replace(u'\xa0', u' ').replace('\r', u'\x1F601').strip().replace(u'\x1F601', '\n'), article_content[article_start:]))
        content_text += ''.join(map(lambda x: x.extract().strip(), article_content[article_start:]))
        contentdict['content'] = content_text
        c = self.conn.cursor()
        # c.execute('''CREATE TABLE reports (id integer primary key, address text, content text, district text, url text, time text, title text)''')

        c.execute('INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?, ?)', (
            contentdict['id'], contentdict['address'], contentdict['content'], contentdict['district'],
            contentdict['url'],
            contentdict['time'], contentdict['title']))
        self.conn.commit()
        reload(sys)
        sys.setdefaultencoding("unicode-escape")
        logging.debug(u''.join(json.dumps(contentdict, indent=True, ensure_ascii=False).replace(u'\\n', u'\n')))

    def parse_results(self, response):

        logging.info(response.url)
        # reports = self.reports(response)
        # pages = self.pages(response)
        # for s in reports:
        #    logging.info("Einsatz: {}".format(response.urljoin(s.extract())))
        # for s in pages:
        #    logging.info("Seite: {}".format(response.urljoin(s.extract())))

        # pass

    def parse_report_pages(self, response):
        for href in self.pages(response):
            url = response.urljoin(href.extract())
            yield scrapy.Request(url, callback=self.parse_report_pages)
