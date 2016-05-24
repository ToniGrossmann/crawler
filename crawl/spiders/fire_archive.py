# -*- coding: utf-8 -*-

import scrapy
from crawl.spiders.fire import FireSpider

class FireArchiveSpider(FireSpider):
    name = "fire_archive"
    allowed_domains = ["www.berliner-feuerwehr.de"]
    start_urls = (
        'http://www.berliner-feuerwehr.de/aktuelles/einsatzarchiv/',
    )

    def __init__(self):
        FireSpider.__init__(self)
        self.parse = self.parse_monthly_reports

    def archive_months(self, response):
        return response.xpath('//div[@class="news-amenu-container"]/ul/li/a/@href')

    def parse_monthly_reports(self, response):
        archive_months = self.archive_months(response)
        for month in archive_months:
            url = response.urljoin(month.extract())
            yield scrapy.Request(url, self.parse_reports)