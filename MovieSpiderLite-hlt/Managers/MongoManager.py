import pymongo
from Config import Config

"""
用于管理mongodb数据库的类
"""
class DBManager(object):

    def __init__(self):
        """
        初始化，创建数据库连接
        :param src:
        :param dest:
        """
        self.client = pymongo.MongoClient(Config.MONGO_URL)
        self.db = self.client[Config.MONGO_DB]

    def save_data(self, col_name, data):
        """
        将数据存储到指定数据库总
        :param col_name:
        :param data:
        :return:
        """
        col = self.db[col_name]
        if col.insert(data):
            print('存储到MongoDB成功', data)
            return True
        else:
            print("无法插入到目标数据库")
            return False

    def get_data(self, col_name):
        """
        从指定数据库获取全部数据并返回
        :param col_name:
        :return:
        """
        col = self.db[col_name]
        datas = col.find({})
        return datas

    def get_count(self, col_name):
        """
        获取指定数据库documents的总数并返回
        :param col_name:
        :return:
        """
        col = self.db[col_name]
        return col.find({}).count()

    def find_data(self, col_name, attr, data):
        """
        在指定数据库中查询字段值符合的数据
        :param col_name:
        :param director:
        :param actor:
        """
        col = self.db[col_name]
        col.create_index(attr)
        tuples = col.find({attr: data}, {'Comments': 1, 'reviews': 1})
        print(tuples)
        if tuples.count() is 0:
            print('数据库中没有指定数据：', attr, '=', data)
            return False
        else:
            return True

    def delete_data(self, col_name, id):
        """
        删除指定数据库的指定id数据
        :param id:
        :param col_name:
        :return:
        """
        col = self.db[col_name]
        if col.remove(id):
            print('成功删除', id)
            return True
        return False

    def delete_data_ignor_case(self, col_name, name, url):
        """
        删除指定数据库中的数据，用于数据去重
        :param col_name:
        :param name:
        :param url:
        :return:
        """
        x = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
        y = range(0, 26)
        z = dict(zip(x, y))
        src_col = self.db[col_name]
        if name[0] in x:
            i = z[name[0]]
            datas = self.get_data(Config.TABLES[i])
            for data in datas:
                movieName = data['movieName'].replace(' I ', '1').replace('II', '2').replace('III', '3').lower().replace('the', '').replace('and', '&').replace('[', '').replace(']', '').replace('/', '').replace('-', '').replace('~', '').replace('(', '').replace(')', '').replace(':', '').replace('\'', '').replace('vs.', 'versus').replace('.', '').replace(',', '').replace('!', '').replace('?', '').replace(' ', '')
                if movieName == name and data['url'] != url and src_col.find_one(data):
                    print("找到源文件")
                    return data
            return None
        else:
            datas = self.get_data(Config.TABLES[26])
            for data in datas:
                movieName = data['movieName'].replace(' I ', '1').replace('II', '2').replace('III', '3').lower().replace('the', '').replace('and', '&').replace('[', '').replace(']', '').replace('/', '').replace('-', '').replace('~', '').replace('(', '').replace(')', '').replace(':', '').replace('\'', '').replace('vs.', 'versus').replace('.', '').replace(',', '').replace('!', '').replace('?', '').replace(' ', '')
                if movieName == name and data['url'] != url and src_col.find_one(data):
                    print("找到源文件")
                    return data
            return None

    def clear_db(self, col_name):
        """
        清空指定数据库
        :param col_name:
        :return:
        """
        col = self.db[col_name]
        col.remove({})

    def sort(self, col_name, attr_name, rank_type):
        """
        根据特定属性对指定数据库排序
        :param col_name:
        :param attr_name:
        :param rank_type: 1 表示升序，-1表示降序
        :return:
        """
        src_col = self.db[col_name]
        datas = src_col.find({}).sort([(attr_name, rank_type)])
        if datas:
            print("排序成功")
            return datas

    def copy_data(self, src, dest):
        """
        将source数据库中的数据全部复制到destination数据库
        :return:
        """
        src_col = self.db[src]
        dest_col = self.db[dest]
        datas = src_col.find({})
        for data in datas:
            if dest_col.insert(data):
                print("复制成功", data)

    def move_data(self, src, dest, count):
        """
        将source数据库中的数据移动count个到destination数据库
        :param count: 要移动的数据量
        :return flag: 0代表移动成功，1代表中间出现删除失败，2代表数据转移全部失败
        """
        src_col = self.db[src]
        dest_col = self.db[dest]
        datas = src_col.find({})
        _count = 1
        flag = -1
        for data in datas:
            if _count > count:
                print("转移完成")
                flag = 0
                break
            if dest_col.insert(data):
                if src_col.remove(data):
                    print("复制并删除成功", data)
                    flag = 0
                else:
                    print("复制成功，删除失败", data)
                    flag = 1
                    break
            else:
                print("数据转移失败", data)
                flag = 2
                break
            _count = _count + 1

        return flag

    def update_attr(self, src, url, attr_name, data):
        """
        为指定数据库更新字段，指定内容
        :param src:
        :param attr_name:
        :return:
        """
        if self.find_data(src, 'url', url):
            src_col = self.db[src]
            src_col.create_index('url')
            if src_col.update({'url': url}, {"$set": {attr_name: data}}):
                print("成功更新字段", attr_name, "内容为", data)
                return True
            else:
                print(id, "无法更新字段", attr_name)
                return False
        else:
            return False

    def delete_attr(self, src, attr_name):
        """
        为指定数据库删除指定字段
        :param src:
        :param attr_name:
        :return:
        """
        src_col = self.db[src]
        if src_col.update({}, {"$unset": {attr_name: ''}}):
            print("成功删除字段", attr_name)
        else:
            print("无法删除字段")

    def change_attr_name(self, src, attr_name, data):
        """
        为指定数据库修改字段名
        :param src:
        :param attr_name:
        :return:
        """
        src_col = self.db[src]
        if src_col.update({}, {"$rename": {attr_name: data}}, False, True):
            print("成功将字段", attr_name, "改名为", data)
        else:
            print("无法修改字段名")
