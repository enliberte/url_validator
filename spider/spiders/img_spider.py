# -*- coding: utf-8 -*-

from scrapy import Request
from scrapy.linkextractors import LinkExtractor

from .base_spider import UrlValidator
from ..items import Link
# from url_validator.url_validator.items import Link
# from url_validator.url_validator.report import HtmlReport


class ImgValidator(UrlValidator):
    seen_images = {}

    def scraped_img(self, response):
        img = response.meta['item']
        img['status_code'] = response.status
        return img

    def seen_img(self, img_url, response):
        abs_img_url = response.urljoin(img_url)
        img = self.seen_images[abs_img_url]
        img['text'] = 'Image (%s)' % img_url
        img['referer_url'] = response.url
        return img

    def scrape_invalid_img(self, failure):
        response = failure.value.response
        yield self.scraped_img(response)

    def scrape_img(self, response):
        yield self.scraped_img(response)

    def get_image(self, img_url, response, callback, errback):
        abs_img_url = response.urljoin(img_url)
        img = Link(text='Image (%s)' % img_url, url=abs_img_url, referer_url=response.url)
        request = Request(abs_img_url, callback=callback, errback=errback)
        request.meta['item'] = img
        self.seen_images[abs_img_url] = img
        return request

    def process_img(self, img_url, response, callback, errback):
        img_url = img_url.strip("\t\r\n ")  # убираем пробельные символы из url
        if img_url in self.seen_images:
            return self.seen_img(img_url, response)
        return self.get_image(img_url, response, callback, errback)

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

        # извлекаем картинки
        images = response.css('img').xpath('@src').getall()

        # обрабатываем картинки
        for image in images:
            processing_result = self.process_img(image, response, self.scrape_img, self.scrape_invalid_img)
            yield processing_result

        # обрабатываем внешние ссылки
        for external_link in external_links:
            processing_result = self.process_link(external_link, response, self.scrape_link, self.scrape_invalid_link)
            yield processing_result

        # обрабатываем внутренние ссылки
        for internal_link in internal_links:
            processing_result = self.process_link(internal_link, response, self.parse, self.scrape_invalid_link)
            yield processing_result


