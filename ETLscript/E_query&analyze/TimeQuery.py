from D_data_transfer.MySQLManager import *

def query_movie_count_by_genre():
    mysql = MysqlManager()
    sql = 'select day_of_week, count(1) c from time group by day_of_week order by c desc'
    results = mysql.execute_query(sql)
    count = 0
    for result in results:
        if count > 50:
            break
        print(result)
        count += 1
    mysql.close_connection()




def main():
    start = datetime.datetime.now()

    query_movie_count_by_genre()

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()