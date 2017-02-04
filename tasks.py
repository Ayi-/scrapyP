#!/usr/bin/env python
# coding=utf-8

from celery import Celery
# from pybloomfilter import BloomFilter
from celery.signals import beat_init
import celeryconfig
import crawl
import pymysql
import logging
import weakref
import redis

app = Celery('task', broker='redis://localhost:6379/0')

app.config_from_object('celeryconfig')


querySQL={
    'QUERY_ARTICLES_URL' : 'select url from articles',
    'QUERY_URL_URL' : 'select url from url',
}



@beat_init.connect
def initredis(*args,**kwargs):
    """
    init bloomFilter
    :param args:
    :param kwargs:
    :return:
    """
    logging.warning('init')
    initRedisURL.delay({'bf':querySQL['QUERY_ARTICLES_URL'],'bfURL':querySQL['QUERY_URL_URL']})

    #  {'bf': <tasks.BloomFilterT object at 0x7fafeaaadcd0>, 'bfURL': <tasks.BloomFilterT object at 0x7fafeab1cb50>}

@app.task
def initRedisURL(item):
    logging.warning(item)
    bf= BloomFilterT.getBF()
    for k,v in item.items():
        logging.warning(k+v)
        bf.initRedis(k,v)

def getConn():
    """
    创建mysql连接
    :return:
    """
    database_conf = app.conf.get('DATABASE')
    conn = pymysql.connect(
        host=database_conf['host'],
        port=int(database_conf['port']),
        user=database_conf['user'],
        passwd=database_conf['password'],
        db=database_conf['database'],
        charset="utf8",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    return conn


# class CacheBFManager:
#     """
#     类缓存管理
#     """
#
#     def __init__(self):
#         self._cache = weakref.WeakValueDictionary()
#
#     def getBF(self,name):
#         if name not in self._cache:
#             bf = BloomF._new(name)
#             self._cache[name] = bf
#         else:
#             bf = self._cache[name]
#         return bf
#
# class BloomF(object):
#     """
#     布隆过滤器
#     """
#     maneger = CacheBFManager()
#     # __slots__ = ['bf']
#     def __init__(self, *args, **kwargs):
#         raise RuntimeError(u"Can't instantiate directly,use getBF()")
#
#     @classmethod
#     def _new(cls, name):
#         self = cls.__new__(cls)
#         self.name = name
#         self.bf  = BloomFilter(100000, 0.01, None)
#         # 创建布隆过滤器
#         conn = getConn()
#         # 获取已经保存的所有url
#         queryUrl = """select url from articles"""
#         with conn.cursor() as cursor:
#             cursor.execute(queryUrl)
#             result = cursor.fetchall()
#             for url in result:
#                 print repr(url.get('url'))
#                 print self.bf.add(url.get('url'))
#         conn.close()
#         return self
#
#     def getBF(self,name):
#         return self.maneger.getBF(name)
#
# bf = BloomF._new('bf')

class CacheBFManager:
    """
    类缓存管理
    """

    def __init__(self):
        self._cache = weakref.WeakValueDictionary()

    def getBF(self, obj, name):
        if name not in self._cache:
            bf = obj._new(name)
            self._cache[name] = bf
        else:
            bf = self._cache[name]
        return bf


class SimpleHash():
    def __init__(self, cap, seed):
        self.cap = cap
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(value.__len__()):
            ret += self.seed * ret + ord(value[i])
        return ((self.cap - 1) & ret)


class BloomFilterT(object):
    """
    布隆过滤器,redis版
    """

    maneger = CacheBFManager()
    iniFlag = False  # 初始化标志

    def __init__(self, *args, **kwargs):
        raise RuntimeError(u"Can't instantiate directly,use getBF()")

    @classmethod
    def _new(cls, name):
        self = cls.__new__(cls)
        self.name = name

        self.bit_size = 1 << 25
        self.seeds = [5, 7, 11, 13, 31, 37, 61]
        self.r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
        self.hashFunc = []
        for i in range(self.seeds.__len__()):
            self.hashFunc.append(SimpleHash(self.bit_size, self.seeds[i]))

        return self

    def initRedis(self,name,sql):
        # 清理数据
        self.r.delete(name)

        # 创建布隆过滤器
        conn = getConn()
        # 获取已经保存的所有url
        # sql = """select url from articles"""
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            for url in result:
                if url.get('url'):
                    self.insert(url.get('url'), name)
        conn.close()

    @classmethod
    def getBF(cls, name='BloomFilterT'):
        return BloomFilterT.maneger.getBF(BloomFilterT, name)

    def isContains(self, str_input, name='url'):
        """

        :param str_input:
        :param name:
        :return:False表示没有记录
        """

        if str_input == None:
            return False
        if str_input.__len__() == 0:
            return False
        ret = True
        for f in self.hashFunc:
            loc = f.hash(str_input)
            ret = ret & self.r.getbit(name, loc)
        return ret

    def insert(self, str_input, name='url'):

        if self.isContains(str_input, name):
            return True
        for f in self.hashFunc:
            loc = f.hash(str_input)
            self.r.setbit(name, loc, 1)
        return False






@app.task
def recommendUrl():
    """
    根据数据库中推荐列表来爬取网页
    :return:
    """

    checkSpider = crawl.checkSpider
    conn = getConn()
    queryUrl = """select id,url,website from url order by website"""
    result = []
    with conn.cursor() as cursor:
        # 查询文章
        cursor.execute(queryUrl)
        result = cursor.fetchall()
        # print result
    if len(result) > 0:

        r = []  # 保存转换结果
        website = ''  # 当前website
        noSpiderUrl = []  # 保存不支持爬虫的url_id
        # result = [{'url':'zhihu.com/1','website':'zhihu','id':'1'},
        #           {'url':'zhihu.com/2','website':'zhihu','id':'2'},
        #           {'url':'douban.com/3','website':'douban','id':'3'},]
        # 将其转化为r
        # r = [{'urls':{'zhihu.com/1':'1','zhihu.com/2':'2'},'website':'zhihu'},
        #      {'urls':{'douban.com/3'},'website':'douban'},]
        for item in result:
            itemWebsite = item['website']
            # 检查是否支持爬虫
            if checkSpider(itemWebsite):
                # 如果在布隆过滤器里面，则跳过
                if app.bf.insert(item['url'], 'queryArticlesUrl'):
                    continue

                if itemWebsite == website:
                    # 添加时，url为key，id为value

                    r[-1]['urls'][item['url']] = item['id']
                else:
                    r.append({'urls': {item['url']: item['id']}, 'website': itemWebsite})
                    website = itemWebsite
            else:
                noSpiderUrl.append(item['id'])
        # crawl.run_spider(r)  # 开始爬数据
        # 修改代码，避免twisted出错
        crawer = crawl.RecommendUrlCrawler()
        crawer.crawl(r)
        setUrlFlag(noSpiderUrl, '3')  # 设置不支持爬虫标签
    conn.close()


@app.task
def TopicUrl():
    """
    获取知乎话题的tag，并整合成url

    :return:
    """
    conn = getConn()
    queryUrl = """select tag_id from tags"""
    result = []

    with conn.cursor() as cursor:
        # 添加文章
        cursor.execute(queryUrl)
        result = cursor.fetchall()

    r = []
    if len(result) > 0:
        for item in result:
            # https://www.zhihu.com/topic/19552832/top-answers
            # 将话题tag转换成url
            if item['tag_id'] == '1':
                continue
            r.append(''.join(('https://www.zhihu.com/topic/', item['tag_id'], '/top-answers')))
        print r
        # crawl.topicSpider(r, bf)
        # 修改代码，避免twisted出错
        crawer = crawl.TopicUrlCrawler()

        crawer.crawl(r, 5)
    conn.close()


@app.task
def WeiBoUrl(url=None):
    """
    获取微博分享的知乎链接
    :return:
    """
    if not url:
        url = [u'http://weibo.com/p/1002065359070375/home?from=page_100206&mod=TAB&is_all=1#place']
    crawer = crawl.WeiBoUrlCrawler()
    crawer.crawl(url)


@app.task
def setUrlFlag(urlList, flag):
    """
    设置urlList中的url的flag，
    包括设置为不在重新爬取
    :param urlList:
    :param flag:
    :return:
    """
    if urlList:
        conn = getConn()
        updateUrlFlag = """UPDATE `url` SET `flag`=%s WHERE id in (%s);"""
        format_strings = ','.join(['%s'] * len(urlList))
        updateUrlFlagsql = updateUrlFlag % ('%s', format_strings)
        urlList.insert(0, flag)
        with conn.cursor() as cursor:
            cursor.execute(updateUrlFlagsql, urlList)
        conn.close()
