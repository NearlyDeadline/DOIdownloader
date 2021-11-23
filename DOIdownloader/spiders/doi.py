# -*- coding: utf-8 -*-
# @Time    : 2021/11/22 22:15
# @Author  : Mike
# @File    : doi
import scrapy
from scrapy import Request
from DOIdownloader.items import DoidownloaderItem


class DoiSpider(scrapy.Spider):
    name = 'doi'
    allowed_domains = ['doi.org']
    start_urls = ['https://doi.org/']
    default_request_headers = {
        'referer': 'https://dblp.uni-trier.de/',
        'sec-ch-ua': '"Microsoft Edge";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.53'
    }

    def __init__(self, *args, **kwargs):
        self.doi_data = kwargs['doi_data']

    def parse(self, response):
        for doi_url, doi_value in self.doi_data:
            yield Request(url=doi_url, callback=self.item_download, headers=self.default_request_headers
                          , dont_filter=True, meta={'type': doi_value['type'], 'paper_id': doi_value['paper_id']})

    def item_download(self, response):
        item = DoidownloaderItem()
        item['content'] = response.text
        item['type'] = response.meta['type']
        item['paper_id'] = response.meta['paper_id']
