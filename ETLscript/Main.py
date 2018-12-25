import time

from config import *
from A_process_txt.A_main import A_main
from B_spider.B_main import B_main
from C_integration.C_main import C_main
from D_data_transfer import import_data_neo4j

import util


def main():
    """
    主函数
    :return:
    """

    # A.获取源数据
    A_main()

    # B.爬虫
    # 为多线程处理数据分表
    util.split_by_num(mongo_user=WTX_USER_str, mongo_db=WTX_DB_str, from_col='source', num=10000)
    # 多线程爬虫
    B_main(thread_num=26, loop=10000, use_proxy=True, use_cookie=True)

    # C.整合数据
    C_main()

    # 将数据导入MySQL数据库

    # 将数据导入Neo4j数据库


