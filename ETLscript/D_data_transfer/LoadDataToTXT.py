from D_data_transfer.MongoManager import *
from D_data_transfer.MySQLManager import *
import random
import datetime
import re
import time


def write_to_txt(path, content):
    """
    将参数中的内容写入txt文件
    :return:
    """
    with open(path, 'a', encoding='utf-8') as f:
        f.write(content + '\n')
        # f.close()


def truncate_txt(path):
    """
    清空txt文件
    :return:
    """
    with open(path, 'a', encoding='utf-8') as f:
        f.truncate(0)
        print('成功清空txt')
        f.close()


def load_movie():
    """
    将mongo中的电影信息按照一定格式写入txt文件
    :return:
    """
    mm = DBManager()
    truncate_txt('/Users/mustafa/Desktop/movie.txt')
    datas = mm.get_data(config.COL_MovieWithAttr_str)
    count = 0
    for data in datas:
        print('current rows: ', count)
        rating = 'NR'
        page_type = '0'
        star = '0'
        duration = '0 minute'
        url = data['url']
        name = data['movieName']
        if 'rating' in data:
            rating = data['rating']
        if data['type'] is 'B':
            page_type = '1'
        if 'star' in data:
            star = data['star'].replace(' out of 5 stars', '')
        if 'duration' in data and 'min' in data['duration']:
            duration = data['duration']
        comments = str(data['Comments'])
        if data['Comments'] is None:
            # print('------------comments is null')
            comments = '0'
        version_count = str(random.randint(1, 7))
        row = url + '，' + name + '，' + rating + '，' + page_type + '，' + star + '，' + duration + '，' + comments + '，' + version_count
        write_to_txt('/Users/mustafa/Desktop/movie.txt', row)
        count += 1


def find_longest_movie_name():
    """
    在mongo中找到电影名最长的电影/超过200的电影
    :return:
    """
    mm = DBManager()
    datas = mm.get_data(config.COL_MovieWithAttr_str)
    max = 200
    count = 1
    time = 0
    for data in datas:
        length =len(data['movieName'])
        if length > max:
            time += 1
            print(count, length, data['url'], data['movieName'])
        count += 1
    print('电影名长度超过200的有', time)


def sync_mysqlID_to_mongo():
    """
    将mysql中的电影id 同步到mongo
    方便后续生成其他维度表的txt
    :return:
    """
    mongo = DBManager()
    mysql = MysqlManager()
    sql = 'select id, name from movie order by id'
    results = mysql.execute_query(sql)
    for result in results:
        mongo.update_attr(config.COL_MovieWithAttr_str, result['name'], 'mysqlID', result['id'])


def load_genre():
    """
    将mongo中的电影类型信息按照一定格式写入txt文件
    :return:
    """
    mm = DBManager()
    truncate_txt('/Users/mustafa/Desktop/genre.txt')
    datas = mm.get_data(config.COL_MovieWithAttr_str)
    count = 1
    for data in datas:
        if 'genres' in data and data['genres']:
            for genre in data['genres']:
                if genre is not '':
                    print('add row', count)
                    row = genre + '，' + str(data['mysqlID']) + '，' + data['movieName']
                    write_to_txt('/Users/mustafa/Desktop/genre.txt', row)
                    count += 1


def load_studio():
    """
    将mongo中的电影工作室信息按照一定格式写入txt文件
    :return:
    """
    mm = DBManager()
    # truncate_txt('/Users/mustafa/Desktop/studio.txt')
    datas = mm.get_data(config.COL_MovieWithAttr_str)
    count = 1
    max = 1
    for data in datas:
        if 'studio' in data and data['studio']:
            for studio in data['studio']:
                if studio is not '':
                    print('add row', count)
                    row = studio + '，' + str(data['mysqlID']) + '，' + data['movieName']
                    write_to_txt('/Users/mustafa/Desktop/studio.txt', row)
                    count += 1


def load_actor():
    """
    将mongo中的演员信息按照一定格式写入txt文件
    :return:
    """
    mm = DBManager()
    truncate_txt('/Users/mustafa/Desktop/actor.txt')
    datas = mm.get_data(config.COL_MovieWithAttr_str)
    count = 0
    max = 1
    for data in datas:
        if 'actors' in data and data['actors']:
            for actor in data['actors']:
                if actor is not '':
                    print('add row', count)
                    row = actor + '，' + str(data['mysqlID']) + '，' + data['movieName']
                    write_to_txt('/Users/mustafa/Desktop/actor.txt', row)
                    count += 1


def load_director():
    """
    将mongo中的导演信息按照一定格式写入txt文件
    :return:
    """
    mm = DBManager()
    # truncate_txt('/Users/mustafa/Desktop/director.txt')
    datas = mm.get_data(config.COL_MovieWithAttr_str)
    count = 0
    max = 1
    for data in datas:
        if 'directors' in data and data['directors']:
            for director in data['directors']:
                if director is not '':
                    print('add row', count)
                    row = director + '，' + str(data['mysqlID']) + '，' + data['movieName']
                    write_to_txt('/Users/mustafa/Desktop/director.txt', row)
                    count += 1


def calculate_day_of_week(year, month, day):
    """
    根据输入的年月日自动计算当天是星期几并返回
    :param year: 年份，int
    :param month: 月份，int
    :param day: 日期，str
    :return: 星期，str
    """
    if month < 10:
        _month = '0' + str(month)
    else:
        _month = str(month)
    if int(day) < 10:
        day = '0' + day
    date = str(year) + _month + day
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(date, "%Y%m%d"))).weekday() + 1


def load_time():
    """
    将mongo中的电影时间信息按照一定格式写入txt文件
    :return:
    """
    truncate_txt('/Users/mustafa/Desktop/time.txt')
    mongo = DBManager()
    datas = mongo.get_data(config.COL_MovieWithAttr_str)
    months = [
        'January',
        'February',
        'March',
        'April',
        'May',
        'June',
        'July',
        'August',
        'September',
        'October',
        'November',
        'December'
    ]
    count = 1
    for data in datas:
        # if 'releaseTime' in data and data['releaseTime'] == 'NR':
        #     mongo.update_attr(config.COL_MovieWithAttr_str, data['movieName'], 'releaseTime', '')
        month = 0
        day = '0'
        day_of_week = '0'
        if 'releaseTime' in data and data['releaseTime'] is not '':
            time = data['releaseTime']
            # 当字段中有','，说明当前字段同时有年、月、日，分割后存储
            if ',' in time:
                time = re.split(',', time)
                year = int(time[1])
                for i in range (0, 12):
                    if months[i] in time[0]:
                        month = i + 1
                        day = time[0].replace(months[i], '').replace(' ', '')
                        day_of_week = calculate_day_of_week(year, month, day)
                        break
            # 当字段中没有','，说明当前字段只有年份，将月份、日期置空后存储
            else:
                # print(count, time)
                year = int(time)
            row = str(year) + '，' + str(month) + '，' + day + '，' + str(day_of_week) + '，' + str(data['mysqlID']) + '，' + data['movieName']
            print('add row', count)
            write_to_txt('/Users/mustafa/Desktop/time.txt', row)
            count += 1


def load_cooperation():
    """
    将mongo中的演员合作信息按照一定格式写入txt文件
    :return:
    """
    mongo = DBManager()
    datas = mongo.get_data(config.COL_MovieWithAttr_str)
    count = 0
    for data in datas:
        if 'actors' in data and data['actors'] != '' and len(data['actors']) > 1:
            list = data['actors']
            for i in range(0, len(list)):
                actor1 = list[i]
                for j in range(i+1, len(list)):
                    actor2 = list[j]
                    row = actor1 + '，' + actor2 + '，' + str(data['mysqlID']) + '，' + data['movieName']
                    count += 1
                    print('add row', count, row)
                    # write_to_txt('/Users/mustafa/Desktop/coop.txt', row)


def load_work_with():
    """
    将mongo中的演员、导演信息按照一定格式写入txt文件
    :return:
    """
    mongo = DBManager()
    datas = mongo.get_data(config.COL_MovieWithAttr_str)
    count = 0
    for data in datas:
        if 'actors' in data and data['actors'] != '' and 'directors' in data and data['directors'] != '':
            list_a = data['actors']
            list_d = data['directors']
            for i in range(0, len(list_a)):
                actor = list_a[i]
                for j in range(0, len(list_d)):
                    director = list_d[j]
                    row = actor + '，' + director + '，' + str(data['mysqlID']) + '，' + data['movieName']
                    count += 1
                    print('add row', count, row)
                    write_to_txt('/Users/mustafa/Desktop/work.txt', row)


def main():
    start = datetime.datetime.now()

    load_work_with()

    end = datetime.datetime.now()
    print("调度总时长：", end - start)


if __name__ == '__main__':
    main()