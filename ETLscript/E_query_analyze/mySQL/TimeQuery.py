from D_data_transfer.MySQLManager import *

def query_day_of_week():
    """
    查询星期几电影数量最多
    :return:
    """
    mysql = MysqlManager()
    sql = 'select day_of_week, count(1) c from time group by day_of_week order by c desc'
    for i in range(0, 1000):
        results = mysql.execute_query(sql)
    # for result in results:
    #     print(result)
    mysql.close_connection()


def query_year():
    """
    查询哪年电影数量最多
    :return:
    """
    mysql = MysqlManager()
    sql = 'select year, count(1) c from time group by year order by c desc;'
    for i in range(0, 1000):
        results = mysql.execute_query(sql)
    # for result in results:
    #     print(result)
    mysql.close_connection()


def query_month():
    """
    查询某年某月的电影数量
    :return:
    """
    mysql = MysqlManager()
    sql = 'select year, month, count(1) c from time where month>0 group by year, month order by c desc;'
    for i in range(0, 1000):
        results = mysql.execute_query(sql)
    # for result in results:
    #     print(result)
    mysql.close_connection()


def main():
    start = datetime.datetime.now()

    query_month()

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()