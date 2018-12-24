from D_data_transfer.MySQLManager import *

def query_movie_count_by_genre():
    mysql = MysqlManager()
    sql = 'select name, count(*) c from genre group by name order by c desc'
    results = mysql.execute_query(sql)
    count = 0
    for result in results:
        if count > 50:
            break
        print(result)
        count += 1
    mysql.close_connection()