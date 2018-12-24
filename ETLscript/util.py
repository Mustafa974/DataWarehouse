import pymongo
import mysql.connector as mc
from config import *


def get_mongodb(mongo_user, mongo_db):
    return pymongo.MongoClient[mongo_user][mongo_db]


def get_mysql_conn(user, password, database):
    return mc.connect(user=user, password=password, database=database)


def split_by_num(mongo_user: str, mongo_db: str, from_col: str, num: int):
    """
    按数量分表
    :param mongo_user:
    :param mongo_db:
    :param from_col:
    :param num:
    :return:
    """
    count = 0
    col = 0
    db = pymongo.MongoClient[mongo_user][mongo_db]
    from_ = db[from_col]
    for each in from_.find():
        db[str(col)].insert_one(each)
        count += 1
        if count == num:
            col += 1
            count = 0


def merge_detail_cols():
    """
    分表合并
    :return:
    """
    for i in range(26):
        db = pymongo.MongoClient[WTX_USER_str][WTX_DB_str]
        # 创建索引
        db['detail_' + str(i)].create_index([(URL, pymongo.HASHED)], unique=True)
        db['detail_' + str(i)].create_index([(TITLE, pymongo.HASHED)])
        flag = False
        # 遍历分表
        for each in db['detail_' + str(i)].find():
            each.pop('_id')
            # print(each)

            # 插入总表
            db[COL_DETAIL_ALL_str].insert_one(each)
            if not flag:
                # 创建索引
                db[COL_DETAIL_ALL_str].create_index([(URL, pymongo.HASHED)], unique=True)
                db[COL_DETAIL_ALL_str].create_index([(TITLE, pymongo.HASHED)])
                flag = True
