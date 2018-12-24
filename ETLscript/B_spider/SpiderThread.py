from B_spider.ParsePage import *
from pyquery import PyQuery as pq
import time
from B_spider.Logger import *
from B_spider.HtmlGetter import *
from B_spider.MongoOP import *
from B_spider.Ticker import *


class SpiderThread(threading.Thread):
    """
    爬虫线程
    """
    def __init__(self, thread_id, loop, use_proxy=False, use_cookie=False, timeout=10):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.loop = loop
        self.use_proxy = use_proxy
        self.use_cookie = use_cookie
        self.timeout = timeout
        # 操作对象
        self.logger = AmazonLogger('log.txt', self.thread_id)
        self.getter = HtmlGetter(thread_id)
        self.mongo = MongoOP(WTX_USER_str, WTX_DB_str)
        self.parser = ParsePage()
        # 集合
        self.my_table = self.mongo.db[str(self.thread_id)]
        self.detail_table = self.mongo.db['detail_' + str(self.thread_id)]
        self.other_page_table = self.mongo.db[COL_OTHER_PAGE_str]
        self.not_movie_table = self.mongo.db[COL_NOT_MOVIE_str]

    def run(self):
        self.get_movie_detail(self.loop, self.use_proxy, self.use_cookie, self.timeout)

    def get_movie_detail(self, loop: int, use_proxy=False, use_cookie=False, timeout=10):
        """
        获取页面详情
        :param use_cookie:
        :param use_proxy:
        :param loop: 循环次数
        :param timeout: 请求超时时限
        :return:
        """
        # 机器人验证计数
        robot_count = Ticker(15)
        # 循环操作
        for i in range(loop):
            print('\nloop round', i + 1, '(', self.thread_id, ')')
            # 找出一项 获取url
            item = self.my_table.find_one({TITLE: {'$exists': False}})
            # 如果集合已空
            if item is None:
                self.logger.log_finish()
                self.logger.log('集合' + str(self.thread_id) + '已空')
                break
            url = item[URL]

            # 发出请求并获得doc
            html = self.getter.get_html(url, use_proxy, use_cookie, timeout)
            doc = pq(html)

            # 判断page的类型
            page = self.parser.get_page_type(doc)
            if page == DOG:
                # 404
                self.logger.log_404(url)
                print('                 <<<< <<<< <<<< <<<<      page not found     >>>> >>>> >>>> >>>>')
                # 删除原信息
                self.mongo.migrate_by_pattern(str(self.thread_id), COL_NOT_FOUND_str, {URL: url})
                self.getter.change_proxy()
                print('migrated')
                robot_count.tick()
            elif page == ROBOT:
                # 机器人验证
                self.logger.log_robot()
                print('机器人验证 [', self.thread_id, ']')
                robot_count.tick()
                self.getter.change_proxy()
            else:
                # （可能）正常的页面
                robot_count.reset_tick()
                detail_movie = self.parser.parse_page_fully(doc, page)
                detail_movie[URL] = url
                detail_movie[PAGE] = page
                detail_movie[COUNT] = item[COUNT]

                # 检查doc情况
                if detail_movie[TITLE] == '':
                    # 其他页面 舍弃
                    print('其他页面')
                    self.other_page_table.insert_one(item)
                else:
                    # 是电影 保存html
                    # self.save_html_file('html/' + url.replace('https://www.amazon.com/dp/', '') + '.html', html)

                    # 存入数据库
                    self.detail_table.insert_one(detail_movie)
                    print('saved', detail_movie)
                # 删除原信息
                self.my_table.delete_one({URL: url})
                print('deleted')

            # 减缓速度
            time.sleep(1)
            if i == loop - 1:
                self.logger.log_finish()
            if not robot_count.is_alive():
                self.logger.log_block()
                print('被block，退出程序\n')
                exit()

    @staticmethod
    def save_html_file(pathname, html):
        """
        保存html文件
        :param pathname: 路径
        :param html: 文件
        :return:
        """
        f = open(pathname, 'w')
        f.write(html)
        f.close()


