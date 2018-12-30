from config import *
from A_process_txt.get_source import *
from B_spider.SpiderThread import SpiderThread
from A_process_txt.import_data import *
from C_integration.Deduplicate import *
from C_integration.Filter import *
import util


def main():
    """
    主函数
    :return:
    """

    """
        A.处理 txt 文件，获取源数据
    """
    # 获得一个评论详情集合（txt）
    import_data_from_txt('movies.txt')

    # 获得一个dvd的ID集合（source）
    get_source_from_file('movies.txt')

    """
        B.爬虫
    """
    # 为多线程处理数据分表
    util.split_by_num(mongo_user=WTX_USER_str, mongo_db=WTX_DB_str, from_col='source', num=10000)
    # 多线程爬虫
    thread_num = 26
    loop = 10000
    use_proxy = True
    use_cookie = True
    for i in range(thread_num):
        SpiderThread(i, loop, use_proxy, use_cookie).start()

    """
        C.整合数据
    """
    # 分表合并
    util.merge_detail_cols()
    # 筛选 去除不是电影的数据
    Filter(COL_DETAIL_ALL_str, COL_NOT_MOVIE_str).main()
    # 去重
    Deduplicate(COL_DETAIL_ALL_str, COL_VERSION_str).main()

    """
        D.导入数据库
    """
    # 将数据导入MySQL数据库

    # 将数据导入Neo4j数据库




