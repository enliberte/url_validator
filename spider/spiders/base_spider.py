# -*- coding: utf-8 -*-
import json
from urllib.parse import urljoin

from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.python import to_native_str
from ..items import Link
from ..report import HtmlReport, CsvReport
# from url_validator.url_validator.items import Link
# from url_validator.url_validator.report import HtmlReport


class UrlValidator(Spider):
    name = 'url_validator'
    domain = 'domain_to_validate'
    start_url = 'start_url'
    allow_internal = []
    allow_external = []
    deny_internal = []
    deny_external = []
    headers = {'Content-type': 'application/json; charset=UTF-8;'}
    exceptions = ['']
    auth = None
    handle_httpstatus_list = [301, 302]
    seen = {}
    db_path = 'links.db'
    html_report = 'report.html'
    csv_report = 'report.csv'

    def seen_link(self, current_link, response):
        link = self.seen[current_link.url]
        link['text'] = current_link.text
        link['referer_url'] = response.url
        return link

    def scraped_link(self, response):
        link = response.meta['item']
        link['url'] = response.url
        link['status_code'] = response.status
        return link

    def scrape_link(self, response):
        yield self.scraped_link(response)

    def scrape_invalid_link(self, failure):
        response = failure.value.response
        yield self.scraped_link(response)

    def make_request(self, current_link, response, callback, errback):
        link = Link(text=current_link.text, referer_url=response.url)
        request = Request(current_link.url, callback=callback, errback=errback)
        request.meta['item'] = link
        return request

    def process_link(self, current_link, response, callback, errback):
        if current_link.url in self.seen:
            return self.seen_link(current_link, response)
        return self.make_request(current_link, response, callback, errback)

    def process_redirection(self, response):
        # HTTP header is ascii or latin1, redirected url will be percent-encoded utf-8
        location = to_native_str(response.headers['location'].decode('latin1'))

        # get the original request
        request = response.request
        # and the URL we got redirected to
        redirected_url = urljoin(request.url, location)

        if response.status in (301, 307) or request.method == 'HEAD':
            redirected = request.replace(url=redirected_url)
            return redirected
        else:
            redirected = request.replace(url=redirected_url, method='GET', body='')
            redirected.headers.pop('Content-Type', None)
            redirected.headers.pop('Content-Length', None)
            return redirected

    def first_request(self):
        link = Link(text='Start Page', referer_url='Start URL')
        request = Request(self.start_url, callback=self.parse)
        request.meta['item'] = link
        return request

    def crawl_start_url(self, response):
        yield self.first_request()

    def empty_callback(self, response):
        yield

    def login_request(self, site, priority, callback):
        params = {"data": {"s": [{"t": "Строка", "n": "login"}, {"t": "Строка", "n": "password"}],
                           "d": [site['user'], site['password']], "_type": "record"}}

        body = json.dumps({'jsonrpc': '2.0', 'method': site['method'], 'params': params, "id": 1, "protocol": 3})
        return Request(site['url'], method='POST', body=body, headers=self.headers, priority=priority, callback=callback)

    def start_requests(self):
        if self.auth:
            for site in self.auth:
                if self.domain in site['url']:
                    yield self.login_request(site, 1, self.crawl_start_url)
                else:
                    yield self.login_request(site, 2, self.empty_callback)
        else:
            yield self.first_request()

    def parse(self, response):
        # создаем ссылку из ответа
        link = self.scraped_link(response)
        self.seen[link['url']] = link
        yield link

        # обрабатываем редиректы
        if 300 <= response.status < 400:
            redirect_rqst = self.process_redirection(response)
            yield redirect_rqst

        # извлекаем ссылки
        internal_links = LinkExtractor(allow_domains=self.domain,
                                       deny=self.deny_internal,
                                       allow=self.allow_internal).extract_links(response)
        external_links = LinkExtractor(deny_domains=self.domain,
                                       deny=self.deny_external,
                                       allow=self.allow_external).extract_links(response)

        # обрабатываем внешние ссылки
        for external_link in external_links:
            processing_result = self.process_link(external_link, response, self.scrape_link, self.scrape_invalid_link)
            yield processing_result

        # обрабатываем внутренние ссылки
        for internal_link in internal_links:
            processing_result = self.process_link(internal_link, response, self.parse, self.scrape_invalid_link)
            yield processing_result

    @staticmethod
    def close(spider, reason):
        HtmlReport(spider.db_path, spider.html_report, spider.exceptions).create()
        CsvReport(spider.db_path, spider.csv_report).create()