import util
from C_integration.Filter import *
from C_integration.Deduplicate import *


def C_main():
    """
    整合数据
    :return:
    """
    # 分表合并
    util.merge_detail_cols()

    # 筛选 去除不是电影的数据
    Filter(COL_DETAIL_ALL_str, COL_NOT_MOVIE_str).main()

    # 去重
    Deduplicate(COL_DETAIL_ALL_str, COL_VERSION_str).main()

    # 合并数据

