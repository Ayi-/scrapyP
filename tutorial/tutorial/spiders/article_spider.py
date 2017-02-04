# coding=utf-8
import logging

from tutorial import pipelines
import scrapy
from tutorial.items import ZhiHuItem,UrlItem
import datetime
from urlparse import urlparse
from scrapy import Selector
import tasks


log = logging.getLogger('scrapy')
log.setLevel(logging.WARNING)

def getSummary(content, Limitlength=500):
    """
    简单截取文本
    :param content:
    :param Limitlength:
    :return:
    """
    return content[:Limitlength]


class zhihuSpider(scrapy.Spider):
    name = "zhihu"
    allower_domains = ["zhihu.com"]
    # start_urls = [
    #
    #     "https://www.zhihu.com/question/34816524#answer-29953654",
    #     "https://www.zhihu.com/question/20899988/answer/49749466",
    # ]
    # 用来设置过滤，对应于要使用的pipeline
    pipeline = set([
        pipelines.MysqlPipeline,
    ])

    def __init__(self, id, url, *args, **kwargs):
        """

        :param id: 数据库要爬取的url对应的id项目，用于重置flag
        :param url: 要爬取的url
        :param args:
        :param kwargs:
        :return:
        """
        logging.info('test------------------------------------>')
        self.start_urls = url
        self.id = id
        super(zhihuSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        # filename = response.url.split("/")[-1]
        # with open(filename,'wb') as f:
        #     f.write(response.body)

        # 获取知乎回答id
        # 针对微博短链接的分享
        if u"t.cn" in response.request.url:
            answer = response.url.split('/')[-1].split('#')[-1].split("-")[-1]
        else:
            answer = response.request.url.split('/')[-1].split('#')[-1].split("-")[-1]
        # sels = response.xpath('/*[@role="main"]')
        sels = response.xpath('//div[@role="main"]')

        item = ZhiHuItem()

        url = response.meta.get('redirect_urls')
        if url:
            url = url[0]
        else:
            url = response.request.url
        item['tag'] = '1'
        item['url'] = url
        # 获取id来与数据库推荐表里的urlID对应
        item['id'] = self.id.get(url)
# //*[@id="zh-question-title"]/h2
        # item['title'] = sels.css('.zm-item-title').xpath('./text()').extract()
        title = sels.css('.zm-item-title.zm-editable-content').xpath('child::a/text()')
        if not title:
            title = sels.css('.zm-item-title.zm-editable-content').xpath('text()')

        item['title'] = ''.join(title.extract()).strip()
        # sels.css('.zm-item-title.zm-editable-content').xpath('./*/text()').extract()
        # sels.css('.zm-item-title.zm-editable-content').xpath('text()|./*/text()')
        # sels.css('.zm-item-title.zm-editable-content').xpath('text()|child::a').extract()[0]

        # print item['title']
        sel = sels.xpath('//div[@data-aid="%s"]' % answer)

        if not sel:
            sel = sels.xpath('//div[@data-atoken="%s"]' % answer)
        if sel:
            sel = sel[0]
            content = sel.css('.zm-editable-content').xpath('./*/text()|./text()')

            # item['content'] = content
            item['content'] = getSummary(''.join(content.extract()).strip(), 100)
            # print item['content']
            # item['author']= sel.css('.author-link').xpath('./text()').extract()[0]
            # item['author']= sel.css('.author-link').xpath('./text()').extract()
            item['author'] = sel.xpath('./div/@data-author-name')[0].extract()

            # print item['author']
            # item['date']= sel.css('.answer-date-link').xpath('./text()').extract()[0].split(" ")[1]
            date = sel.css('.answer-date-link').xpath('./text()').extract()
            item['date'] = "".join(date).split(' ')[-1]
        else:
            item['title'] = None
        yield item


class zhihuTopicSpider(scrapy.Spider):

    name = "zhihuTopic"
    allower_domains = ["zhihu.com"]
    # start_urls = [
    #
    #     "https://www.zhihu.com/question/34816524#answer-29953654",
    #     "https://www.zhihu.com/question/20899988/answer/49749466",
    # ]
    # 用来设置过滤，对应于要使用的pipeline
    pipeline = set([
        pipelines.MysqlPipeline,
    ])

    def __init__(self, url, num=5, *args, **kwargs):
        """

        :param num: 要获取前几个话题<20
        :param url: 要爬取的url
        :param args:
        :param kwargs:
        :return:
        """

        self.start_urls = url
        logging.info('test------------------------------------>')
        self.num = num if num < 20 else 20
        self.bf = tasks.BloomFilterT.getBF('bf')
        # print id(self.bf)

        super(zhihuTopicSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        # 获取前5个话题
        sels = response.css('.feed-item.feed-item-hook.folding')
        parsed_uri = urlparse(response.url)
        domain = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
        #domain = response.meta['download_slot']
        # 计数
        count = 1
        tag = response.url.split('/')[-2]

        # print "####################"
        for sel in sels:
            if count>self.num:
                break
            url = domain + sel.css('.zm-item-rich-text.js-collapse-body').xpath('./@data-entry-url').extract()[0]
            url = unicode(url)
            # print repr(url)
            # print '1----->' ,url in self.bf
            # print len(self.bf)
            if self.bf.insert(url,'queryArticlesUrl'):
                continue
            # print '2----->' ,url in self.bf

            count += 1
            item = ZhiHuItem()
            item['title'] = sel.css('.question_link').xpath('./text()').extract()[0]
            author = sel.css('.author-link')
            if author:
                author = author.xpath('./text()').extract()[0]
            else:
                author = u"匿名用户"
            item['author'] = author
            content = ''.join(sel.css('.zh-summary.summary').xpath('./text()').extract())
            item['content'] = content.strip()
            item['url'] = url
            item['tag'] = tag
            item['id'] = None
            item['date'] = datetime.datetime.today().strftime("%Y-%m-%d")

            yield item


class WeboUrlSpider(scrapy.Spider):
    """
    爬取软件学会微博分享的知乎链接
    """

    name = "WeboUrl"
    custom_settings = {'USER_AGENT':'google spider'}
    # 用来设置过滤，对应于要使用的pipeline
    pipeline = set([
        pipelines.WeiBoUrlPipeline,
    ])

    def __init__(self, url,*args, **kwargs):
        """

        :param num: 要获取前几个话题<20
        :param url: 要爬取的url
        :param args:
        :param kwargs:
        :return:
        """

        #log.warning("="*30)
        #log.warning(url)
        logging.info('test------------------------------------>')
        self.start_urls = url  # u'http://weibo.com/p/1002065359070375/home?from=page_100206&mod=TAB&is_all=1#place'
        self.bfURL = tasks.BloomFilterT.getBF('bfURL')

        super(WeboUrlSpider, self).__init__(*args, **kwargs)
        #log.warning("="*30)
        #log.warning(type(self.start_urls))

    def parse(self, response):
        # 获取所有微博
        divlist=response.xpath('//body').re('<div[^<>]*class="WB_text W_f14"[^<>]*>[\s\S]*?</div>')
        log = logging.getLogger('scrapy')
        log.warning(response)
        log.warning("=="*30)

        for div in divlist :
            if u'知乎' in div:
                log.warning("start=========================>")
                sel = Selector(text=div, type="html")
                # 获取每个微博里的链接
                href = sel.xpath('//@href')
                if len(href) > 0:
                    url = href[0].extract()

                    # 如果不在里面
                    if not self.bfURL.insert(url,'queryUrlUrl'):
                        log.warning(url)
                        item = UrlItem()
                        item['url'] = url
                        yield item

