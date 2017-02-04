#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import  # 这一行极为重要，不添加会导致crontab import失败
from datetime import timedelta
from celery.schedules import crontab  # 周期性任务
import logging
# 用来设置celery消息通道，使用redies
BROKER_URL = 'redis://localhost:6379/0'
# 设置数据存储
# CELERY_RESULT_BACKEND = 'redis://localhost/0

# he log level output to stdout and stderr is logged as. Can be one of DEBUG, INFO, WARNING, ERROR or CRITICAL.
CELERY_REDIRECT_STDOUTS_LEVEL = logging.WARNING

# If enabled stdout and stderr will be redirected to the current logger.
# Enabled by default. Used by celery worker and celery beat.
CELERY_REDIRECT_STDOUTS = False

# 设置序列化格式
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
# 设定时区
TIME_ZONE = 'UTC'
USE_TZ = True
CELERY_ENABLE_UTC = True
CELERY_TIMEZONE = 'Asia/Shanghai'
DATABASE = {  # 'host':'119.29.169.15',
    'host': '192.168.56.1',
    'port': '3306',
    'user': 'root',
    'password': '0000',
    'database': 'lysa'}

# 定时任务
# timedelta(seconds=30),定时30秒
CELERYBEAT_SCHEDULE = {
    'spider_recommend': {
        'task': 'tasks.recommendUrl',
        # 'schedule': crontab(minute='*/15'),
        'schedule': crontab(),
    },
    'spider_topic': {
        'task': 'tasks.TopicUrl',
        # 'schedule': crontab(minute=0, hour='*/3')
        'schedule': crontab(),
    },
    'spider_weiboUrl':{
        'task': 'tasks.WeiBoUrl',
        # 'schedule': crontab(minute=0, hour='*/3')
        'schedule': crontab(),
        'args': [[u'http://weibo.com/p/1002065359070375/home?from=page_100206&mod=TAB&is_all=1#place']],
    }
    # 'test': {
    #     'task': 'tasks.SingTest',
    #     #'schedule': crontab(minute=0, hour='*/3')
    #     'schedule': crontab(),
    #     'args': "g",
    # },
}
