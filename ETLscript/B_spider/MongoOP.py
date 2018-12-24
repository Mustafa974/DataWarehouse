import pymongo
import threading


class MongoOP:
    """
    进行一些mongodb数据库操作
    """
    def __init__(self, user_name, db_name):
        self.client = pymongo.MongoClient(user_name)
        self.db = self.client[db_name]

    def insert_with_check(self, col_name, item, check_key=None):
        col = self.db[col_name]
        if check_key is None:
            if col.find(item).count() > 0:
                return False
        else:
            if col.find({check_key: item[check_key]}).count() > 0:
                return False
        col.insert_one(item)
        return True

    def migrate_by_num(self, from_name, to_name, num):
        from_ = self.db[from_name]
        to = self.db[to_name]
        count = 0
        for each in from_.find():
            to.insert_one(each)
            from_.delete_one({'url': each['url']})
            count += 1
            if count == num:
                return

    def migrate_by_pattern(self, from_name, to_name, pattern):
        from_ = self.db[from_name]
        to = self.db[to_name]
        for each in from_.find(pattern):
            to.insert_one(each)
            from_.delete_one(pattern)

    def split_by_num(self, form_name, num):
        count = 0
        col = 0
        from_ = self.db[form_name]
        for each in from_.find():
            self.db[str(col)].insert_one(each)
            count += 1
            if count == num:
                col += 1
                count = 0


class ColThread(threading.Thread):
    def __init__(self, user_name, db_name, col_name):
        threading.Thread.__init__(self)
        client = pymongo.MongoClient(user_name)
        db = client[db_name]
        self.col = db[col_name]

    def run(self):
        pass
