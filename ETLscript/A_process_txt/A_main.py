from A_process_txt.get_source import *
from A_process_txt.import_data import *


def A_main():
    """
    处理 txt 文件，获取源数据
    :return:
    """
    # 获得一个评论详情集合（txt）
    import_data_from_txt('movies.txt')

    # 获得一个dvd的ID集合（source）
    get_source_from_file('movies.txt')
