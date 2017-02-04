# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TutorialItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class ZhiHuItem(scrapy.Item):
    title = scrapy.Field()
    content = scrapy.Field()
    author = scrapy.Field()
    date = scrapy.Field()
    id = scrapy.Field()
    url = scrapy.Field()
    tag = scrapy.Field()

    def __repr__(self):
        """only print out attr1 after exiting the Pipeline"""
        # return repr({"id": self['id'],"title":self['title']})
        return repr({"url": self['url'],"tag":self['tag'],"title":self['title']})


class UrlItem(scrapy.Item):
    url = scrapy.Field()
    website = scrapy.Field()  # ，默认为zhihu
    flag = scrapy.Field()  # 默认为0

    def __repr__(self):
        return repr({'url':self['url']})