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
    col.create_index('star')
    # col.create_index('Comments')
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
    col.create_index('releaseTime.day_of_week')
    result = col.aggregate(pipeline)
    # pprint.pprint(list(result))


def time2(col):
    """
    1999年有哪些电影
    :param col:
    :return:
    """
    # select year, movie_name from time where year=1999;
    col.create_index('releaseTime')
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
    col.create_index('directors')
    results = col.find({'directors': 'Jim Edward'})
    for result in results:
        # print(result)
        pass


def actor1(col):
    """
    根据演员名查询演过的电影
    :param col:
    :return:
    """
    # select name, movie_name from actor where name='Wendy Braun';
    col.create_index('actors')
    results = col.find({'actors': 'Wendy Braun'})
    for result in results:
        # print(result)
        pass


def genre1(col):
    """
    某个类型的全部电影
    :param col:
    :return:
    """
    # select name, movie_name from genre where name='Action';
    col.create_index('genres')
    results = col.find({'genres': 'Action'})
    for result in results:
        # print(result)
        pass


def studio1(col):
    """
    某个工作室的全部电影
    :param col:
    :return:
    """
    # select name, movie_name from studio where name='CreateSpace';
    col.create_index('studio')
    results = col.find({'studio': 'CreateSpace'})
    for result in results:
        # print(result)
        pass


def comb1(col):
    """
    某个演员和某个导演合作过的电影及其评分、热度
    :param col:
    :return:
    """
    # with movies as (select movie_name from work_with where actor_name='Mel Blanc' and director_name='Friz Freleng') select movie.name, movie.star, movie.review from movie, movies where movie.name=movies.movie_name order by movie.star desc, movie.review desc;
    col.create_index('directors')
    col.create_index('actors')
    col.create_index('star')
    results = col.find({'actors': 'Mel Blanc', 'directors': 'Friz Freleng'}).sort([('star', -1)])
    for result in results:
        # print(result)
        pass


def comb4(col):
    """
    某演员参演的电影详情（按评分排序）
    :param col:
    :return:
    """
    # select movie.name, movie.rated, movie.star, movie.review, movie.version_count, movie.duration from movie, actor where actor.name='Morgan Freeman' and actor.movie_name=movie.name order by star desc;
    results = col.find({'actors': 'Morgan Freeman'}).sort([('star', -1)])
    col.create_index('actors')
    col.create_index('star')
    for result in results:
        # print(result['movieName'], result['actors'], result['star'])
        pass


def comb5(col):
    """
    哪个月份的电影热度最高
    :param col:
    :return:
    """
    # select month, avg(review) avg_review from movie, time where movie.name=time.movie_name group by month order by avg_review desc;
    pipeline = [
        {"$unwind": "$releaseTime.month"},
        {"$group": {"_id": "$releaseTime.month", "avg_review": {"$avg": "$Comments"}}},
        {"$sort": SON([("avg_review", -1)])}
    ]
    col.create_index('releaseTime.month')
    # col.create_index('Comments')
    result = col.aggregate(pipeline)
    # pprint.pprint(list(result))


def cp1(col):
    """
    某类型电影的黄金上映期
    :param col:
    :return:
    """
    # with movies as (select movie.name movie_name, movie.review review, genre.name genre from movie, genre where movie.name=genre.movie_name and genre.name='Action' order by review desc limit 100) select movies.movie_name, movies.review, movies.genre, time.year, time.month, time.day from movies, time where time.movie_name=movies.movie_name;
    col.create_index('genres')
    # col.create_index('Comments')
    results = col.find({'genres': 'Action'}).sort([('Comments', -1)])
    for result in results:
        # print(result['movieName'], result['genres'], result['Comments'], result['releaseTime'])
        pass


def cp3(col):
    """
    某工作室拍过的某个类型的电影详情
    :param col:
    :return:
    """
    # with movies as (select movie.name, movie.rated, movie.star, movie.review, movie.version_count, movie.duration from movie, studio where studio.name='20th Century Fox' and movie.name=studio.movie_name) select movies.name, movies.rated, movies.star, movies.review, movies.version_count, movies.duration from movies, genre where genre.name='Action' and movies.name=genre.movie_name order by movies.star desc;
    col.create_index('studio')
    col.create_index('genres')
    col.create_index('star')
    results = col.find({'studio': '20th Century Fox'}, {'genres': 'Action'}).sort([('star', -1)])
    for result in results:
        # print(result['movieName'], result['genres'], result['Comments'], result['releaseTime'])
        pass


def main():
    mongo = DBManager()
    col = mongo.db[config.COL_MovieWithAttr_str]
    start = datetime.datetime.now()

    for i in range(1000):
        cp3(col)

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()
