import pymongo

from config import *


def get_source_from_file(path):
    """
    从txt文件中获取ID，生成爬虫所需的url，存入目标集合source
    :return:
    """
    source_col = WTX_DB_str[COL_SOURCE_str]
    count_keyword = 0
    cur_id = ''

    file = open(path, encoding='ISO-8859-1')
    for line in file:
        line = line.strip('\n')

        if 'productId' in line:
            productId = line.split(': ')[1]
            if cur_id == '':
                cur_id = productId
                print('cur_id:' + cur_id)

            if cur_id != productId:
                # finish scanning all the reviews of same dvd
                save_to_db(source_col, 'https://www.amazon.com/dp/' + cur_id, count_keyword)
                print('count_keyword:', count_keyword)
                # refresh
                count_keyword = 0
                cur_id = productId
                print('cur_id:' + cur_id)

        if 'review/summary' in line or 'review/text' in line:
            count_keyword += line.count('movie', 0, len(line)) + line.count('film', 0, len(line))
    file.close()
    # 创建索引
    source_col.create_index([(URL, pymongo.ASCENDING)], unique=True)


def save_to_db(col, url, count):
    movie = {
        'url': url,
        'count': count
    }
    col.insert_one(movie)
