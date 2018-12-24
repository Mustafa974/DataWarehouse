#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql.cursors
from config import *
import datetime
from D_data_transfer.MongoManager import *

class MysqlManager(object):

    def __init__(self):
        """
        对象初始化，针对同一个对象只创建一次mysql连接以及游标
        """
        self.connection = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USR, password=MYSQL_PWD,
                                          db=MYSQL_DB, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.connection.cursor()

    def insert(self):
        sql = 'insert into t_student values (%s, %s, %s)'
        self.cursor.execute(sql)

    def update(self):
        sql = 'update t_student set name = %s where student_id = %s'
        self.cursor.execute(sql)

    def delete(self):
        sql = 'delete from t_student where name = %s'
        self.cursor.execute(sql)

    def execute_sql(self, sql):
        """
        执行参数中的sql语句并提交
        若出现异常则回滚
        :param sql:
        :return:
        """
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            print('成功执行sql语句', sql)
        except:
            self.connection.rollback()
            print('无法执行sql语句', sql)

    def execute_query(self, sql):
        """
        执行参数中的查询语句
        通过游标获取全部查询结果并返回list
        :param sql: 查询sql语句
        :return: 查询结果list（每一条结果都是dict类型）
        """
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def execute_query_with_para(self, sql, paras):
        """
        执行参数中的查询语句
        通过游标获取全部查询结果并返回list
        :param sql: 查询sql语句
        :return: 查询结果list（每一条结果都是dict类型）
        """
        self.cursor.execute(sql, paras)
        result = self.cursor.fetchall()
        return result

    def close_connection(self):
        self.connection.close()


def main():
    start = datetime.datetime.now()

    mysql = MysqlManager()
    # sql = 'delete from director where name="Variou"'
    # mysql.execute_sql(sql)
    # Variou N a * Mul Na non -
    # sql = 'select name, count(id) c from director group by name order by c desc'
    sql = 'select name, count(*) c from genre group by name order by c desc'
    results = mysql.execute_query(sql)
    count = 0
    for result in results:
        if count > 50:
            break
        print(result)
        count += 1
    mysql.close_connection()

    end = datetime.datetime.now()
    print("调度总时长：", end - start)


if __name__ == '__main__':
    main()


# movie : 82879
# genre : 19469
# studio : 78499
# actor : 285993
# director : 91766
# time : 20384，有星期的1.2w
# cooperate : 483138
# work_with : 344166
