import requests
from Config import Config
from requests import RequestException


# 动态获取代理的类
class Proxy(object):

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/68.0.3440.106 Safari/537.36'
        }

    def get_proxy(self):
        """
        从本地代理接口随机获取一个可用代理
        :return:
        """
        response = requests.get(Config.PROXY_URL, headers=self.headers)
        try:
            if response.status_code == 200:
                my_proxy = {
                    "http": response.text
                }
                return my_proxy
            else:
                print("无法访问localhost")
                return None
        except RequestException:
            print('请求代理失败')
            return None


    def test_proxy(self, proxy):
        """
        测试指定代理是否有效
        :param proxy:
        :return:
        """
        response = requests.get(Config.PROXY_TEST_URL, headers=self.headers, proxies=proxy)
        try:
            if response.status_code == 200:
                return True
            return False
        except RequestException:
            print('请求详情页失败', Config.PROXY_TEST_URL)
            return False
