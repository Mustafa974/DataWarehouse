from config import *
from B_spider.MongoOP import *
import re
from difflib import SequenceMatcher as sm


class Deduplicate:
    def __init__(self, src_col: str, version_col: str):
        self.client = pymongo.MongoClient[WTX_USER_str]
        self.db = self.client[WTX_DB_str]
        self.src_col = self.db[src_col]
        self.version_col = self.db[version_col]

    def main(self, loop=-1):
        if loop == -1:
            for item in self.src_col.find({CHECKED: {'$exists': False}}).sort(REVIEW, pymongo.DESCENDING):
                self.deduplicate_one(item)
        else:
            for i in range(loop):
                item = self.src_col.find({CHECKED: {'$exists': False}}).sort(REVIEW, pymongo.DESCENDING).next()
                self.deduplicate_one(item)

    def deduplicate_one(self, item):
        """
        为一部电影去重
        :param item:
        :return:
        """
        # 记录检查
        self.src_col.update_one({URL: item[URL]}, {CHECKED: True})
        # 取关键词
        kws = item[TITLE].split()
        kw = kws[0]
        for one in kws:
            if one.lower() != 'a' and one.lower() != 'the' and one.lower() != 'an':
                kw = one
                break

        # 根据关键词搜索电影
        for each in self.src_col.find({TITLE: {'$regex': '.*'+kw+'.*'}, CHECKED: {'$exists': False}}):
            title_count = 0
            for word in kws:
                if word in each[TITLE]:
                    title_count += 1

            # 如果标题相似度达到0.5以上或有包含关系
            if title_count/len(kws) >= 0.5 or item[TITLE] in each[TITLE] or each[TITLE] in item[TITLE]:
                # 计算二者总体相似度
                ratio = self.cmp_items(item, each)
                if ratio < 0.7:
                    # 相似度不高 可能是两部电影 先跳过
                    # public.log('diff(' + str(ratio) + '): ' + each[TITLE] + ' ' + each[URL])
                    print('diff(' + str(ratio) + '): ' + each[TITLE] + ' ' + each[URL])
                else:
                    # 相似度较高 认为是同一部电影 转移到version集合
                    each[REF_URL] = item[URL]
                    self.version_col.insert_one(each)
                    self.src_col.delete_one({URL: each[URL]})
                    # public.log('same(' + str(ratio) + '): ' + each[TITLE] + ' ' + each[URL])
                    print('same(' + str(ratio) + '): ' + each[TITLE] + ' ' + each[URL])

    @staticmethod
    def cmp_items(item1: dict, item2: dict):
        """
        比较两个item相似度
        :param item1:
        :param item2:
        :return:
        """
        # title 相似度
        r_title = sm(None, item1[TITLE].lower(), item2[TITLE].lower()).ratio()
        # 导演
        if DIRECTORS in item1 and DIRECTORS in item2:
            r_dir = Deduplicate.cmp_list(item1[DIRECTORS], item2[DIRECTORS])
        else:
            r_dir = 0.5
        # 演员
        if ACTORS in item1 and ACTORS in item2:
            r_act = Deduplicate.cmp_list(item1[ACTORS], item2[ACTORS])
        else:
            r_act = 0.5
        # count评论
        if item1[COUNT] == item2[COUNT] and item1[COUNT] != 0:
            r_count = 0.3
        else:
            r_count = 0

        total = 0.4*r_title + 0.3*r_dir + 0.3*r_act + r_count
        return total

    @staticmethod
    def cmp_list(list1: list, list2: list):
        """
        比较两个列表相似度
        :param list1:
        :param list2:
        :return:
        """
        len1 = len(list1)
        len2 = len(list2)
        list1.sort()
        list2.sort()
        mark = 0
        if len1 <= len2:
            min_l = list1
            max_l = list2
        else:
            max_l = list1
            min_l = list2
        for s in min_l:
            if s in max_l:
                mark += 1
            else:
                for t in max_l:
                    if sm(None, s.lower(), t.lower()).ratio() > 0.9:
                        mark += 1
        return (mark/len1 + mark/len2)*0.5

    @staticmethod
    def clean_title1(title: str) -> str:
        """
        清除title中多余的信息
        :param title:
        :return:
        """
        patterns = [
            r'\(.*?\)',
            r'\[.*?\]',
            'VHS',
            'DVD',
            'Complete Collection',
            'Collection',
            'Pack',
            'Edition',
            'Collector\'s',
            # 'Region \d',
            # 'V.\d+',
            # 'Part \d+',
            # 'Volume \d+',
            # 'Vol. \d+',
            # 'Vol \d+',
            # 'Season \d+',
        ]
        for pattern in patterns:
            title = re.sub(pattern, '', title, flags=re.IGNORECASE)
        title = title.strip(' .\':;*')
        return title

