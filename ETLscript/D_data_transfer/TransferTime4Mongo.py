from D_data_transfer.MongoManager import *
from D_data_transfer.MySQLManager import *

def transfer_time_4_mongo():
    """
    将mysql中的年月日星期的数据导入mongo
    :return:
    """
    mongo = DBManager()
    mysql = MysqlManager()
    sql = 'select movie_id, year, month, day, day_of_week from time order by movie_id;'
    results = mysql.execute_query(sql)
    count = 1
    for result in results:
        # if count > 10:
        #     break
        time = {
            'year': result['year'],
            'month': result['month'],
            'day': result['day'],
            'day_of_week': result['day_of_week']
        }
        print(count, '\t', result['movie_id'], '\t', time)
        mongo.update_attr(config.COL_MovieWithAttr_str, result['movie_id'], 'releaseTime', time)
        count += 1


def main():
    transfer_time_4_mongo()


if __name__ == '__main__':
    main()