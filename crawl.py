#!/usr/bin/env python
# coding=utf-8
from scrapy import log
from scrapy.crawler import CrawlerProcess

from twisted.internet import reactor
from billiard import Process
from scrapy.utils.project import ENVVAR, get_project_settings
import sys
import os
from scrapy.utils.log import configure_logging
sys.path.append('tutorial')
from tutorial.spiders.article_spider import zhihuSpider, zhihuTopicSpider,WeboUrlSpider


# class UrlCrawlerScript(Process):
#     def __init__(self, spider):
#         Process.__init__(self)
#         settings = get_project_settings()
#         self.crawler = CrawlerProcess(spider,settings)
#         self.crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
#         self.spider = spider
#
#     def run(self):
#         self.crawler.crawl(self.spider)
#         self.crawler.start()
#         reactor.run()

# 记录website对应的爬虫
spider = {
    'http://www.zhihu.com': zhihuSpider,

}

os.environ.setdefault(ENVVAR, 'tutorial.settings')
settings = get_project_settings()


class Crawler():
    def __init__(self):
        self.crawler = CrawlerProcess(settings)


class RecommendUrlCrawler(Crawler):
    """
    爬取推荐url
    """

    def _crawl(self, urlResult):
        for urls in urlResult:
            sp = spider.get(urls.get('website'))

            self.crawler.crawl(sp, id=urls['urls'], url=urls['urls'].keys())

        self.crawler.start()
        self.crawler.stop()

    def crawl(self, urlResult):
        p = Process(
            target=self._crawl,
            args=[urlResult]
        )
        p.start()
        p.join()


class TopicUrlCrawler(Crawler):
    """
    爬取知乎话题精华
    """

    def _crawl(self, url, num):
        self.crawler.crawl(zhihuTopicSpider, url=url, num=num)

        self.crawler.start()
        self.crawler.stop()

    def crawl(self, url, num):
        p = Process(
            target=self._crawl,
            args=[url, num]
        )
        p.start()
        p.join()


class WeiBoUrlCrawler(Crawler):
    """
    爬取微博知乎分享的链接
    """

    def _crawl(self, url):
        self.crawler.crawl(WeboUrlSpider, url=url)

        self.crawler.start()
        self.crawler.stop()

    def crawl(self, url):
        p = Process(
            target=self._crawl,
            args=[url]
        )
        p.start()
        p.join()


def checkSpider(website):
    """
    检查website对应的spider是否存在
    :param spi:
    :return:
    """
    return True if spider.get(website, None) else False

#
# def run_spider(urlResult):
#     os.environ.setdefault(ENVVAR, 'tutorial.settings')
#     setting = get_project_settings()
#     process = CrawlerProcess(setting)
#     for urls in urlResult:
#         # print item
#         sp = spider.get(urls.get('website'))
#         process.crawl(sp, id=urls['urls'], url=urls['urls'].keys())
#
#     process.start()  # the script will block here until all crawling jobs are finished
#     # crawler = UrlCrawlerScript(spider)
#     # crawler.start()
#     # crawler.join()
#
#
# def topicSpider(urltag):
#     os.environ.setdefault(ENVVAR, 'tutorial.settings')
#     setting = get_project_settings()
#     process = CrawlerProcess(setting)
#
#     process.crawl(zhihuTopicSpider, urltag, 5)
#     process.start()
