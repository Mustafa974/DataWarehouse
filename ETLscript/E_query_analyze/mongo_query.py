from D_data_transfer.MongoManager import *
import datetime

def time1(col):
    """
    星期几电影数量最多
    :param col:
    :return:
    """
    dayOfWeek = ['1', '2', '3', '4', '5', '6', '7']
    counts = [0, 0, 0, 0, 0, 0, 0]
    for data in col.find({'releaseTime.day_of_week': {"$in": ('1', '2', '3', '4', '5', '6', '7')}}):
        # print(data)
        for j in range(0, 7):
            if data['releaseTime']['day_of_week'] == dayOfWeek[j]:
                counts[j] += 1
                break
    # print(counts)


def time2(col):
    """
    哪年电影数量最多
    :param col:
    :return:
    """
    years = {}
    for data in col.find({"releaseTime": {'$exists': True}}):
        if data['releaseTime'] == '':
            continue
        # print(data)
        year = data['releaseTime']
        year = year['year']
        # print(data['mysqlID'], year)
        years.setdefault(year, 0)
        years[year] += 1
    # print(years)


# def movie1(col):
    """
    版本数最多的电影+评论数>100
    :param col:
    :return:
    """



def main():
    mongo = DBManager()
    col = mongo.db[config.COL_MovieWithAttr_str]
    start = datetime.datetime.now()

    for i in range(10):
        movie1(col)

    end = datetime.datetime.now()
    print(end - start)


if __name__ == '__main__':
    main()
