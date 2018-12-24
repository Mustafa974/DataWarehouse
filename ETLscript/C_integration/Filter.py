from config import *
import re
import pymongo


class Filter:
    def __init__(self, src_col: str, dump_col: str):
        self.client = pymongo.MongoClient[WTX_USER_str]
        self.db = self.client[WTX_DB_str]
        self.src_col = self.db[src_col]
        self.dump_col = self.db[dump_col]

    def main(self, loop=-1):
        if loop == -1:
            for item in self.src_col.find({MARK: {'$exists': False}}):
                self.process_one_item(item)
        else:
            for i in range(loop):
                item = self.src_col.find_one({MARK: {'$exists': False}})
                self.process_one_item(item)

    def process_one_item(self, item: dict):
        """
        处理一个item
        :param item:
        :return:
        """
        # 清除导演和演员中无效的数据
        item[DIRECTORS] = self.clean_people(item[DIRECTORS])
        if not item[DIRECTORS]:
            item.pop(DIRECTORS)
        item[ACTORS] = self.clean_people(item[ACTORS])
        if not item[ACTORS]:
            item.pop(ACTORS)

        # 类别 & 标题
        if (GENRES in item and self.check_genres(item[GENRES]) is False) \
                or (TITLE in item and self.check_title(item[TITLE]) is False):
            item[REASON] = 'not expected genre'
            self.dump_col.insert_one(item)
            self.src_col.delete_one({URL: item[URL]})
            print('deleted')
            return

        # 记分
        marks = self.mark(item)
        item[MARK] = marks
        if marks >= 400:
            print(item[URL], '[', marks, ']', 'maybe a movie')
            self.src_col.update_one({URL: item[URL]}, {'$set': item})
            print('updated')
        else:
            print(item[URL], '[', marks, ']', 'may not a movie')
            item[REASON] = 'low mark'
            self.dump_col.insert_one(item)
            # 删除
            self.src_col.delete_one({URL: item[URL]})
            print('deleted')
        return

    @staticmethod
    def mark(item: dict) -> int:
        """
        给item打分，分数越高越可能是电影
        :param item:
        :return: 分数
        """
        marks = 0
        # 黑色页面很大可能是电影
        if PAGE in item and PAGE == 1:
            marks += 300
        # 评论
        marks = item[COUNT]
        # 导演
        if DIRECTORS in item:
            marks += 100
        # 演员
        if ACTORS in item:
            marks += 100
        # 电影评级
        rates = ['PG-13', 'PG', 'R', 'G']
        if RATED in item and item[RATED] in rates:
            marks += 200
        return marks

    @staticmethod
    def clean_people(people: list) -> list:
        """
        清除无效数据，如n/a等
        :param people:
        :return:
        """
        for person in people:
            if person.__len__() <= 2 or 'various' in person.lower():
                people.remove(person)
        return people

    @staticmethod
    def check_title(title: str) -> bool:
        """
        检查标题是否含有某些明显表示不是电影的字段
        :param title:
        :return:
        """
        lower_title = title.lower()
        bans = ['classic', 'fitness', 'workout', 'yoga', 'tv', 'beginner', 'lesson', 'class', 'ballet']
        for word in bans:
            if word in lower_title:
                return False
        pattern1 = r'season\d'
        pattern2 = r'vol\d'
        if 'series' in lower_title or 'volume' in lower_title:
            return False
        if re.search(pattern1, lower_title.replace(' ', ''), 0) is not None \
                or re.search(pattern2, lower_title.replace(' ', ''), 0) is not None:
            return False
        return True

    @staticmethod
    def check_genres(genres: list) -> bool:
        """
        检查类型是否含有某些明显表示不是电影的字段
        :param genres:
        :return:
        """
        # 类别
        not_expected = ['Exercise & Fitness', 'Music Videos & Concerts', 'Educational', 'Sports']
        for ele in genres:
            if ele in not_expected or ele.find('TV') != -1:
                # not a movie
                return False
        return True
