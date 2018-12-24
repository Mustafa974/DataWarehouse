from D_data_transfer.MySQLManager import *

def query_day_of_week():
    mysql = MysqlManager()
    sql = 'select day_of_week, count(1) c from time group by day_of_week order by c desc'
    results = mysql.execute_query(sql)
    for result in results:
        print(result)
    mysql.close_connection()


def query_year():
    mysql = MysqlManager()
    sql = 'select year, count(1) c from time group by year order by c desc;'
    results = mysql.execute_query(sql)
    for result in results:
        print(result)
    mysql.close_connection()


def query_month():
    mysql = MysqlManager()
    sql = 'select year, month, count(1) c from time where month>0 group by year, month order by c desc;'
    results = mysql.execute_query(sql)
    for result in results:
        print(result)
    mysql.close_connection()




def main():
    start = datetime.datetime.now()

    query_day_of_week()

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()