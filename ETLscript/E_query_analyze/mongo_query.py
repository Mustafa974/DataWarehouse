from D_data_transfer.MongoManager import *
import datetime

def movie_week_count():
    """
    星期几电影数量最多
    :return:
    """
    mongo = DBManager()
    col = mongo.db[config.COL_MovieWithAttr_str]
    for i in range (1, 100):
        dayOfWeek = ['1', '2', '3', '4', '5', '6', '7']
        counts = [0, 0, 0, 0, 0, 0, 0]
        for data in col.find({'releaseTime.day_of_week': {"$in": ('1', '2', '3', '4', '5', '6', '7')}}):
            # print(data)
            for i in range(0, 7):
                if data['releaseTime']['day_of_week'] == dayOfWeek[i]:
                    counts[i] += 1
                    break
        print(counts)


def movie_year_count():
    """
    哪年电影数量最多
    :return:
    """
    mongo = DBManager()
    col = mongo.db[config.COL_MovieWithAttr_str]


def main():
    start = datetime.datetime.now()

    movie_week_count()

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()
