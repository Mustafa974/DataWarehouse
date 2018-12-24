import csv
import mysql.connector as mc
import util

# mysql
USER = 'amazon'
PASSWORD = 'amazon'
DATABASE = 'AmazonMovie'
S_DATABASE = 's_amazon'

# 把mysql表变成小表
# 导出节点node
# 导出关系rel

#
# def test():
#     get_all_rel_csv()


def generate_neo4j_csv():
    # 建立一个符合第三范式的数据库
    build_s_amazon()

    # 从数据库的表中导出node的csv
    get_all_node_csv()

    # 从数据库的表中导出relationship的csv
    get_all_rel_csv()


def build_s_time():
    """
    创建和时间有关的表
    :return:
    """
    conn = util.get_mysql_conn(USER, PASSWORD, S_DATABASE)
    conn.autocommit = True
    cursor = conn.cursor()

    # day_of_week表
    for i in range(1, 8):
        cursor.execute('insert into s_day_of_week(value) values (%s);', (i,))
    # month表
    for i in range(1, 31):
        cursor.execute('insert into s_month(value) values (%s);', (i,))

    for result in result_cursor('select movie_id,year,month,day,day_of_week from time;'):
        # 年
        if result[1] != 0:
            try:
                cursor.execute('insert into s_year(value) values (%s);', (result[1],))
            except mc.errors.IntegrityError:
                print('重复', 'year')
            try:
                cursor.execute('insert into s_movie_year(movie_id,value) values (%s,%s);', (result[0], result[1]))
            except mc.errors.IntegrityError:
                print('重复', 'movie-year')

        # 月
        if result[2] != 0:
            try:
                cursor.execute('insert into s_movie_month(movie_id,value) values (%s,%s);', (result[0], result[2]))
            except mc.errors.IntegrityError:
                print('重复', 'movie-month')

        # 日
        if result[3] != 0:
            try:
                cursor.execute('insert into s_day(value) values (%s);', (result[3],))
            except mc.errors.IntegrityError:
                print('重复', 'day')
            try:
                cursor.execute('insert into s_movie_day(movie_id,value) values (%s,%s);', (result[0], result[3]))
            except mc.errors.IntegrityError:
                print('重复', 'movie-day')

        # 星期
        if result[4] != 0:
            try:
                cursor.execute('insert into s_movie_week(movie_id,value) values (%s,%s);', (result[0], result[4]))
            except mc.errors.IntegrityError:
                print('重复', 'movie-week')


def build_s_table(source_sql: str, insnode_sql: str, query_id_sql: str, insrel_sql: str, nodename: str, relname: str):
    """
    根据参数建表
    :param source_sql: 查询源数据表的语句
    :param insnode_sql: 插入node表的语句
    :param query_id_sql: 查询node表id的语句
    :param insrel_sql: 插入relationship表的语句
    :param nodename:
    :param relname:
    :return:
    """
    conn = util.get_mysql_conn(USER, PASSWORD, S_DATABASE)
    conn.autocommit = True
    cursor = conn.cursor()
    for result in result_cursor(source_sql):
        try:
            cursor.execute(insnode_sql, (result[0],))
        except mc.errors.IntegrityError:
            print('重复', nodename)
        cursor.execute(query_id_sql, (result[0],))
        Id = cursor.fetchone()[0]
        try:
            cursor.execute(insrel_sql, (result[1], Id))
        except mc.errors.IntegrityError:
            print('重复', relname)


def build_s_amazon():
    """
    创建数据库s_amazon里的所有表
    :return:
    """
    # 演员
    build_s_table(
        source_sql='select name,movie_id from actor;',
        insnode_sql='insert into s_actor(name) values (%s);',
        query_id_sql='select id from s_actor where name = %s;',
        insrel_sql='insert into s_movie_actor(movie_id,actor_id) values (%s,%s);',
        nodename='actor',
        relname='movie-actor'
    )

    # 导演
    build_s_table(
        source_sql='select name,movie_id from director;',
        insnode_sql='insert into s_director(name) values (%s);',
        query_id_sql='select id from s_actor where name = %s;',
        insrel_sql='insert into s_movie_director(movie_id,director_id) values (%s,%s);',
        nodename='director',
        relname='movie-director'
    )

    # 工作室
    build_s_table(
        source_sql='select name,movie_id from studio;',
        insnode_sql='insert into s_studio(name) values (%s);',
        query_id_sql='select id from s_studio where name = %s;',
        insrel_sql='insert into s_movie_studio(movie_id,studio_id) values (%s,%s);',
        nodename='studio',
        relname='movie-studio'
    )

    # 类型
    build_s_table(
        source_sql='select name,movie_id from genre;',
        insnode_sql='insert into s_genre(name) values (%s);',
        query_id_sql='select id from s_genre where name = %s;',
        insrel_sql='insert into s_movie_genre(movie_id,genre_id) values (%s,%s);',
        nodename='genre',
        relname='movie-genre'
    )

    # 时间
    build_s_time()


def get_all_node_csv():
    """
    生成所有node的csv
    :return:
    """
    # 电影
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, DATABASE),
        csv_path='movie.csv',
        row_list=['id:ID', 'name', 'review:int', 'star:float', 'version_count:int', 'duration', ':LABEL'],
        label='movie',
        offset=0,
        query_sql='select id,name,review,star,version_count,duration from movie;'
    )

    # 演员
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='actor.csv',
        row_list=['id:ID', 'name', ':LABEL'],
        label='actor',
        offset=1,
        query_sql='select id,name from s_actor;'
    )

    # 导演
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='director.csv',
        row_list=['id:ID', 'name', ':LABEL'],
        label='director',
        offset=2,
        query_sql='select id,name from s_director;'
    )

    # 类型
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='genre.csv',
        row_list=['id:ID', 'name', ':LABEL'],
        label='genre',
        offset=3,
        query_sql='select id,name from s_genre;'
    )

    # 工作室
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='studio.csv',
        row_list=['id:ID', 'name', ':LABEL'],
        label='studio',
        offset=4,
        query_sql='select id,name from s_studio;'
    )

    # 年
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='year.csv',
        row_list=['id:ID', 'value', ':LABEL'],
        label='year',
        offset=5,
        query_sql='select value from s_year;'
    )

    # 月
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='month.csv',
        row_list=['id:ID', 'value', ':LABEL'],
        label='month',
        offset=6,
        query_sql='select value from s_month;'
    )

    # 日
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='day.csv',
        row_list=['id:ID', 'value', ':LABEL'],
        label='day',
        offset=7,
        query_sql='select value from s_day;'
    )

    # 星期
    get_node_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='day_of_week.csv',
        row_list=['id:ID', 'value', ':LABEL'],
        label='day_of_week',
        offset=8,
        query_sql='select value from s_day_of_week;'
    )


def get_node_csv(conn, csv_path: str, row_list: list, label: str, offset: int, query_sql: str):
    """
    生成一个node的csv
    :param conn:
    :param csv_path:
    :param row_list:
    :param label:
    :param offset:
    :param query_sql:
    :return:
    """
    with open(csv_path, 'w') as f:
        writer = csv.writer(f)
        # 写入表头
        writer.writerow(row_list)
        # 获取表中元组
        for result in result_cursor(query_sql, conn):
            # 构造要写入的信息
            row = list(result)
            if offset >= 5:
                row.append(row[0])
            row.append(label)
            row[0] = offset * 1000000 + result[0]
            # 写入数据
            print(csv_path, '>>>>>', row)
            writer.writerow(row)


def result_cursor(query_sql: str, conn=util.get_mysql_conn(USER, PASSWORD, DATABASE)):
    """
    查询
    :param conn:
    :param query_sql: 查询语句
    :return: 查询结果的迭代器
    """
    cursor = conn.cursor()
    cursor.execute(query_sql)
    values = cursor.fetchall()
    for each in values:
        yield each
    cursor.close()


def get_all_rel_csv():
    """
    生成所有关系relationship的csv
    :return:
    """
    # movie_actor
    get_rel_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='movie_actor.csv',
        row_list=[':START_ID', ':END_ID', ':TYPE'],
        type_='movie_actor',
        query_sql='select movie_id,actor_id from s_movie_actor;',
        offset0=0,
        offset1=1
    )

    # movie_director
    get_rel_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='movie_director.csv',
        row_list=[':START_ID', ':END_ID', ':TYPE'],
        type_='movie_director',
        query_sql='select movie_id,director_id from s_movie_director;',
        offset0=0,
        offset1=2
    )

    # movie_genre
    get_rel_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='movie_genre.csv',
        row_list=[':START_ID', ':END_ID', ':TYPE'],
        type_='movie_genre',
        query_sql='select movie_id,genre_id from s_movie_genre;',
        offset0=0,
        offset1=3
    )

    # movie_studio
    get_rel_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='movie_studio.csv',
        row_list=[':START_ID', ':END_ID', ':TYPE'],
        type_='movie_studio',
        query_sql='select movie_id,studio_id from s_movie_studio;',
        offset0=0,
        offset1=4
    )

    # movie_year
    get_rel_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='movie_year.csv',
        row_list=[':START_ID', ':END_ID', ':TYPE'],
        type_='movie_year',
        query_sql='select movie_id,value from s_movie_year;',
        offset0=0,
        offset1=5
    )

    # movie_month
    get_rel_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='movie_month.csv',
        row_list=[':START_ID', ':END_ID', ':TYPE'],
        type_='movie_month',
        query_sql='select movie_id,value from s_movie_month;',
        offset0=0,
        offset1=6
    )

    # movie_day
    get_rel_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='movie_day.csv',
        row_list=[':START_ID', ':END_ID', ':TYPE'],
        type_='movie_day',
        query_sql='select movie_id,value from s_movie_day;',
        offset0=0,
        offset1=7
    )

    # movie_day_of_week
    get_rel_csv(
        conn=util.get_mysql_conn(USER, PASSWORD, S_DATABASE),
        csv_path='movie_day_of_week.csv',
        row_list=[':START_ID', ':END_ID', ':TYPE'],
        type_='movie_day_of_week',
        query_sql='select movie_id,value from s_movie_week;',
        offset0=0,
        offset1=8
    )


def get_rel_csv(conn, csv_path: str, row_list: list, type_: str, query_sql: str, offset0: int, offset1: int):
    """
    生成关系relationship的csv
    :param conn:
    :param csv_path:
    :param row_list:
    :param type_:
    :param query_sql:
    :param offset0:
    :param offset1:
    :return:
    """
    with open(csv_path, 'w') as f:
        writer = csv.writer(f)
        # 写入表头
        writer.writerow(row_list)
        # 获取表中元组
        for result in result_cursor(query_sql, conn):
            # 构造要写入的信息
            row = list(result)
            row[0] = row[0] + offset0 * 1000000
            row[1] = row[1] + offset1 * 1000000
            row.append(type_)
            # 写入数据
            print(csv_path, '>>>>>', row)
            writer.writerow(row)
