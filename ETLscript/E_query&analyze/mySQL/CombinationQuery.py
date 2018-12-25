from D_data_transfer.MySQLManager import *


def query_coop_actors():
    """
    查询共同参演过电影的演员及其合作次数
    :return:
    """
    mysql = MysqlManager()
    sql = 'select actor_name1, actor_name2, count(1) c from cooperate_with group by actor_name1, actor_name2 order by c desc ;'
    for i in range(0, 10):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def query_coop_actors_rank():
    """
    查询共同参演过电影的演员及其合作次数
    优化后在数据库中新增一个rank表
    :return:
    """
    mysql = MysqlManager()
    sql = 'select actor_name1, actor_name2, coop_times from coop_rank order by coop_times desc limit 100;'
    for i in range(0, 100):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def query_coop_movies():
    """
    查询某两个演员合作过的电影及其评分
    :return:
    """
    mysql = MysqlManager()
    sql = 'select movie.name, movie.star from movie, cooperate_with cw where cw.actor_name1=\'Moe Howard\' and cw.actor_name2=\'Curly Howard\' and cw.movie_name=movie.name order by movie.star desc;'
    # sql = 'with movies as (select movie_name from cooperate_with where actor_name1=\'Moe Howard\' and actor_name2=\'Curly Howard\')select movie_name, star from movies, movie where movies.movie_name=movie.name order by star desc;'
    # sql = 'select movie_name from cooperate_with where actor_name1=\'Moe Howard\' and actor_name2=\'Curly Howard\';'
    for i in range(1, 100):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def query_work_with():
    """
    查询合作过的演员和导演及其合作次数
    :return:
    """
    mysql = MysqlManager()
    sql = 'select actor_name, director_name, count(1) c from work_with group by actor_name, director_name order by c desc;'
    for i in range(1, 10):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def query_work_rank():
    """
    查询合作过的演员和导演及其合作次数
    优化后在数据库中添加rank表
    :return:
    """
    mysql = MysqlManager()
    sql = 'select actor_name, director_name, work_times from work_rank order by work_times desc limit 100;'
    for i in range(1, 100):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def query_avg_star_review_of_director():
    """
    查询某个导演导过电影的平均评分以及平均热度
    :return:
    """
    mysql = MysqlManager()
    sql = 'with movies as (select movie_name from director where name=\'Cerebellum Corporation\') select avg(star) avg_star, avg(review) avg_review from movies, movie where movies.movie_name=movie.name order by avg_star desc, avg_review desc;'
    for i in range(1, 10000):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def query_week_of_movie_of_actor():
    """
    查询某个演员参演过的电影通常在周几上映
    :return:
    """
    mysql = MysqlManager()
    sql = 'select actor.movie_name, time.day_of_week from time, actor where actor.name=\'Jackie Chan\' and actor.movie_name=time.movie_name order by day_of_week;'
    # sql = 'with movies as (select movie_name from actor where name=\'Jackie Chan\') select time.movie_name, day_of_week from movies, time where movies.movie_name=time.movie_name order by day_of_week;'
    for i in range(1, 1000):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def query_dir_studio_coop():
    """
    查询合作过的导演和工作室及其合作次数
    :return:
    """
    mysql = MysqlManager()
    sql = 'select director.name, studio.name, count(1) c from director, studio where director.movie_name=studio.movie_name group by director.name, studio.name order by c desc;'
    for i in range(1, 10):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def query_movie_genre_time():
    """
    查询某类型电影的黄金上映期
    :return:
    """
    mysql = MysqlManager()
    sql = 'select movie.name, genre.name, movie.review, time.year, time.month, time.day from movie, genre, time where genre.name=\'Action\' and genre.movie_name=movie.name=time.movie_name order by movie.review desc limit 100; '
    # sql = 'with movies as (select movie.name movie_name, movie.review review, genre.name genre from movie, genre where movie.name=genre.movie_name and genre.name=\'Action\' order by review desc limit 100) select movies.movie_name, movies.review, movies.genre, time.year, time.month, time.day from movies, time where time.movie_name=movies.movie_name;'
    for i in range(1, 1000):
        results = mysql.execute_query(sql)
    # count = 0
    # for result in results:
    #     if count > 100:
    #         break
    #     print(result)
    #     count += 1
    mysql.close_connection()


def main():
    start = datetime.datetime.now()

    query_movie_genre_time()

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()