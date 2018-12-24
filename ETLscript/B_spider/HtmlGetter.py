import requests
from config import *
from B_spider.ProxyPool import ProxyPool


COOKIE0 = ''

COOKIE1 = ''

COOKIE2 = ''

COOKIE3 = ''

COOKIE4 = ''

COOKIE5 = ''

COOKIE6 = ''

COOKIE7 = ''

COOKIES = [COOKIE0, COOKIE1, COOKIE2, COOKIE3, COOKIE4, COOKIE5, COOKIE6, COOKIE7]


class HtmlGetter:
    """
    获取html
    """
    def __init__(self, thread_id):
        self.proxies = None
        self.thread_id = thread_id

    def get_html(self, url: str, use_proxy=False, use_cookie=False, timeout=10):
        """
        从url获取html文件
        :param url: 要爬取的页面url
        :param use_proxy: 是否使用代理(池)
        :param use_cookie: 是否使用cookie
        :param thread_id: 线程号
        :param timeout: 请求超时时限
        :return: html
        """
        headers = HEADERS
        # 使用cookie
        if use_cookie is True:
            cookie = self.__get_cookie()
            if cookie is not None and cookie != '':
                headers['cookie'] = cookie

        # 使用代理
        if use_proxy is True and self.proxies is None:
            self.change_proxy()

        # 记录 响应+下载 的时间
        # time1 = datetime.datetime.now()
        try:
            if use_proxy is True:
                response = requests.get(url, headers=headers, timeout=timeout, proxies=self.proxies)
            else:
                response = requests.get(url, headers=headers, timeout=timeout)
        except:
            if use_proxy is True:
                self.change_proxy()
            return self.get_html(url, use_proxy, use_cookie, timeout)
        # time2 = datetime.datetime.now()

        # 打印信息
        # if self.proxies is not None:
        #     print('\nuse', self.proxies['http'])
        # print('to get', url, 'response in', time2 - time1)

        return response.text

    def __get_cookie(self):
        """
        获取cookie
        :return: cookie值（string）
        """
        return COOKIES[self.thread_id % 8]

    def change_proxy(self):
        """
        更换代理
        :return:
        """
        self.proxies = ProxyPool.get_proxies()
