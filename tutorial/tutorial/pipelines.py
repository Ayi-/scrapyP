# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import logging
import datetime
import time
import functools


class TutorialPipeline(object):
    def process_item(self, item, spider):
        return item


def check_spider_pipeline(process_item_method):
    """
    edit by http://stackoverflow.com/questions/8372703/how-can-i-use-different-pipelines-for-different-spiders-in-a-single-scrapy-proje
    装饰器
    用于设定不同的pipeline对应不同的spider
    :param process_item_method:
    :return:
    """
    @functools.wraps(process_item_method)
    def wrapper(self, item, spider):

        # message template for debugging
        msg = '%%s %s pipeline step' % (self.__class__.__name__,)

        # if class is in the spider's pipeline, then use the
        # process_item method normally.
        if self.__class__ in spider.pipeline:
            spider.log(msg % 'executing', level=logging.DEBUG)
            return process_item_method(self, item, spider)

        # otherwise, just return the untouched item (skip this step in
        # the pipeline)
        else:
            spider.log(msg % 'skipping', level=logging.DEBUG)
            return item

    return wrapper



class DBBasePipeline(object):
    """
    基础类，添加mysqldb
    """
    def __init__(self,database_conf):
        self.database_conf = database_conf
        self.conn = pymysql.connect(
            host=self.database_conf['host'],
            port=int(self.database_conf['port']),
            user=self.database_conf['user'],
            passwd=self.database_conf['password'],
            db=self.database_conf['database'],
            charset="utf8",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        database_conf = settings.get('DATABASE')
        return cls(database_conf)

class MysqlPipeline(DBBasePipeline):
    """
    保存爬取的数据
    """
    def __init__(self,*args,**kwargs):
        super(MysqlPipeline, self).__init__(*args,**kwargs)

    @check_spider_pipeline
    def process_item(self,item,spider):
        title = item.get('title')
        if title:
            sqlInsetr = """Insert into articles (title,content,author,date,created_at,url,tag) value(%s,%s,%s,%s,%s,%s,%s)"""
            sqlUpdateFlag = """update url set flag = 1 where id = %s"""
            author = item.get('author')
            date = item.get('date')
            id = item.get('id')
            url = item.get('url')
            tag = item.get('tag','1')
            content = item.get('content')
            create_date=time.mktime(datetime.datetime.today().timetuple())
            with self.conn.cursor() as cursor:
                # 添加文章
                cursor.execute(sqlInsetr, (title, content, author, date, create_date, url,tag))
                # 更新Flag
                if id:
                    cursor.execute(sqlUpdateFlag,(id))
        else:
            sqlUpdateFlag = """update url set flag = 3 where id = %s"""
            id = item.get('id')
            with self.conn.cursor() as cursor:

                # 更新Flag
                if id:
                    cursor.execute(sqlUpdateFlag,(id))
        return item


class WeiBoUrlPipeline(DBBasePipeline):
    """
    保存微博爬取到的知乎链接
    保存爬取的数据
    """
    def __init__(self,*args,**kwargs):
        super(WeiBoUrlPipeline, self).__init__(*args,**kwargs)

    @check_spider_pipeline
    def process_item(self,item,spider):
        url = item.get('url')
        #log = logging.getLogger('scrapy')

        if url:
            sqlInsetr = """Insert into url (url,website,flag) value(%s,%s,%s)"""

            flag = item.get('flag','0')
            #log.warning(flag)
            website = 'http://www.zhihu.com'
            #log.warning(url+flag+website)
            with self.conn.cursor() as cursor:
                # 添加url
                cursor.execute(sqlInsetr, (url, website,flag ))
        return item



