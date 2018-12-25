from D_data_transfer.MySQLManager import *


def query_movie_version():
    """
    查询电影版本数、大于100的评论数
    :return:
    """
    mysql = MysqlManager()
    sql = 'select name, review, version_count vc from movie where review>100 order by vc desc, review desc;'
    for i in range(0, 100):
        results = mysql.execute_query(sql)
    # for result in results:
    #     print(result)
    mysql.close_connection()


def query_movie_rated_star():
    mysql = MysqlManager()
    sql = 'select avg(star) avg_star, rated from movie group by rated order by avg_star desc;'
    for i in range(0, 1000):
        results = mysql.execute_query(sql)
    # for result in results:
    #     print(result)
    mysql.close_connection()


def main():
    start = datetime.datetime.now()

    query_movie_rated_star()

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()