from B_spider.SpiderThread import *


def B_main(thread_num=1, loop=1, use_proxy=False, use_cookie=False):
    """
    多线程爬虫
    :return:
    """
    AmazonLogger('log.txt', 0).log_start(thread_num, loop)
    for i in range(thread_num):
        SpiderThread(i, loop, use_proxy, use_cookie).start()
