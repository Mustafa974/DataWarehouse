import pymongo

from config import *


def import_data_from_txt(path):
    """
    从txt文件中获取所需数据并存入目标集合txt
    :param path:
    :param dest:
    :return:
    """

    f = open(path, 'r', encoding='ISO-8859-1')
    src_col = WTX_DB_str[COL_TXT_str]

    try:
        # 生成迭代器，初始化
        iter_f = iter(f)
        cur_url = ''
        old_url = ''
        count = 0  # 记录当前循环次数
        result = []
        userID = ''
        profileName = ''
        helpfulness = ''
        score = ''
        time = ''
        summary = ''
        text = ''

        # 遍历存储全部电影id
        for line in iter_f:
            # 若当前行是目标行则执行数据库存储
            if line.find('product/productId') >= 0:
                # 找到一个新的电影id，循环数加一
                count += 1
                cur_url = 'https://www.amazon.com/dp/' + line.replace('product/productId: ', '').strip('\n') + '/'
                if count is 1:
                    old_url = cur_url
                    continue
                data = {
                    'userID': userID,
                    'profileName': profileName,
                    'helpfulness': helpfulness,
                    'score': score,
                    'time': time,
                    'summary': summary,
                    'text': text
                }
                result.append(data)
                # 若当前电影id与上一个不同，则应将历史数据打包存入数据库
                if cur_url != old_url:
                    print('当前电影Id为', cur_url, '上一部电影id为', old_url, '，存储上一部电影的评论结果数据')
                    # 将result存入数据库
                    src_col.insert_one({'url': old_url, 'reviews': result})
                    result.clear()
                # 重置旧url
                old_url = cur_url

            elif line.find('review/userId') >= 0:
                userID = line.replace('review/userId: ', '').strip('\n')
            elif line.find('review/profileName') >= 0:
                profileName = line.replace('review/profileName: ', '').strip('\n')
            elif line.find('review/helpfulness') >= 0:
                helpfulness = line.replace('review/helpfulness: ', '').strip('\n')
            elif line.find('review/score') >= 0:
                score = line.replace('review/score: ', '').strip('\n')
            elif line.find('review/time') >= 0:
                time = line.replace('review/time: ', '').strip('\n')
            elif line.find('review/summary') >= 0:
                summary = line.replace('review/summary: ', '').strip('\n')
            elif line.find('review/text') >= 0:
                text = line.replace('review/text: ', '').strip('\n')
        # 最后一组数据，循环结束后存入数据库
        data = {
            'userID': userID,
            'profileName': profileName,
            'helpfulness': helpfulness,
            'score': score,
            'time': time,
            'summary': summary,
            'text': text
        }
        result.append(data)
        src_col.insert_one({'url': old_url, 'reviews': result})
    finally:
        if f:
            # 关闭文件
            f.close()
    # 创建索引
    src_col.create_index([(URL, pymongo.ASCENDING)], unique=True)
