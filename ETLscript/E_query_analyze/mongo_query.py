from D_data_transfer.MongoManager import *
from bson.son import SON
import datetime
import pprint

def movie1(col):
    """
    电影热度随评分的变化关系
    :param col:
    :return:
    """
    # select star, avg(review) from movie group by star order by star desc;
    pipeline = [
        {"$unwind": "$star"},
        {"$group": {"_id": "$star", "avg_review": {"$avg": "$Comments"}}},
        {"$sort": SON([("_id", -1), ("avg_review", -1)])}
    ]
    result = col.aggregate(pipeline)
    # pprint.pprint(list(result))


def time1(col):
    """
    星期几电影数量最多
    :param col:
    :return:
    """
    #select day_of_week, count(1) c from time group by day_of_week order by c desc
    pipeline = [
        {"$unwind": "$releaseTime.day_of_week"},
        {"$group": {"_id": "$releaseTime.day_of_week", "count": {"$sum": 1}}},
        {"$sort": SON([("count", -1)])}
    ]
    result = col.aggregate(pipeline)
    # pprint.pprint(list(result))


def time2(col):
    """
    1999年有哪些电影
    :param col:
    :return:
    """
    # select year, movie_name from time where year=1999;
    results = col.find({"releaseTime.year": 1999})
    for result in results:
        # print(result)
        pass


def director1(col):
    """
    根据名字查询导过的电影
    :param col:
    :return:
    """
    # select name, movie_name from director where name='Jim Edward';
    results = col.find({'directors': 'Jim Edward'})
    for result in results:
        # print(result)
        pass


# def director2(col):
#     """
#     找出导演电影最多的（前100）
#     :param col:
#     :return:
#     """
#     # select name, count(1) c from director group by name order by c desc limit 100;
#     pipeline = [
#         {"$unwind": "$directors"},
#         {"$group": {"_id": "directors", "count": {"$sum": 1}}},
#         {"$sort": SON([("count", -1)])}
#     ]
#     result = col.aggregate(pipeline)
#     pprint.pprint(list(result))


def main():
    mongo = DBManager()
    col = mongo.db[config.COL_MovieWithAttr_str]
    start = datetime.datetime.now()

    for i in range(1):
        director2(col)

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()
