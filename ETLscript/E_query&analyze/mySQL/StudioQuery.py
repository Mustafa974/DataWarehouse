from D_data_transfer.MySQLManager import *


def query_studio_count_by_name():
    mysql = MysqlManager()
    sql = 'select name, count(1) from studio where name=\'CreateSpace\';'
    results = mysql.execute_query(sql)
    for result in results:
        print(result)
    mysql.close_connection()


def query_studio_count():
    mysql = MysqlManager()
    sql = 'select name, count(1) c from studio group by name order by c desc;'
    results = mysql.execute_query(sql)
    count = 0
    for result in results:
        # if count > 100:
        #     break
        print(result)
        # count += 1
    mysql.close_connection()



def main():
    start = datetime.datetime.now()

    query_studio_count_by_name()

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()