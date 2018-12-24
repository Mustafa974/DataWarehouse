import requests
from config import *


class ProxyPool:
    """
    从代理池获取代理
    """
    @staticmethod
    def get_proxies():
        """
        获得代理
        :return: 代理proxies
        """
        try:
            response = requests.get(PROXY_POOL_URL)
            if response.status_code == 200 and response.text is not None:
                proxies = {
                    'http': 'http://' + response.text
                }
                return proxies
            return None
        except ConnectionError:
            return None

    @staticmethod
    def test_proxies(proxies, timeout=20):
        """
        测试代理是否可用
        :param proxies: 代理
        :param timeout: 请求超时时限
        :return: 是否可用（string）
        """
        try:
            response = requests.get('https://www.baidu.com', proxies=proxies, timeout=timeout)
            if response.status_code == 200:
                return True
            else:
                return False
        except ConnectionError:
            return False
