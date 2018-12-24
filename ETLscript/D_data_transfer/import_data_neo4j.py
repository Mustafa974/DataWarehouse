from py2neo.data import Relationship
import util
from B_spider.Ticker import Ticker


"""
    neo4j import data 命令:
    bin/neo4j-admin import --nodes import/node/actor.csv --nodes import/node/day_of_week.csv --nodes import/node/day.csv --nodes import/node/director.csv --nodes import/node/genre.csv --nodes import/node/month.csv --nodes import/node/studio.csv --nodes import/node/year.csv --nodes import/node/movie.csv --relationships import/rel/movie_actor.csv --relationships import/rel/movie_day_of_week.csv --relationships import/rel/movie_day.csv --relationships import/rel/movie_director.csv --relationships import/rel/movie_genre.csv --relationships import/rel/movie_month.csv --relationships import/rel/movie_studio.csv --relationships import/rel/movie_year.csv
"""

# mysql
USER = 'amazon'
PASSWORD = 'amazon'
DATABASE = 'AmazonMovie'
S_DATABASE = 's_amazon'
# neo4j
MOVIE_ID = 'movie_id'
MOVIE_NAME = 'movie_name'
LABEL_ACTOR = 'actor'
LABEL_DIRECTOR = 'director'
REL_WORK_WITH = 'work_with'
REL_COOPERATE_WITH = 'cooperate_with'

# 从mysql获得数据
# 从name找到node节点
# 看是否存在关系
# 若存在……若不存在……


def main():
    amazon = Mysql(USER, PASSWORD, DATABASE)
    neo4j = Neo4j(PASSWORD)

    # 导演与演员work_with关系
    for result in amazon.get_results('select actor_name,director_name,movie_id,movie_name from work_with'):
        print('result:', result)
        act = neo4j.find_node_by_name(LABEL_ACTOR, result[0])
        dire = neo4j.find_node_by_name(LABEL_DIRECTOR, result[1])
        neo4j.insert_one_movie(dire, act, REL_WORK_WITH, result[2], result[3])

    for result in amazon.get_results('select actor_name1,actor_name2,movie_id,movie_name from cooperate_with'):
        print('result:', result)
        act1 = neo4j.find_node_by_name(LABEL_ACTOR, result[0])
        act2 = neo4j.find_node_by_name(LABEL_ACTOR, result[1])
        neo4j.insert_one_movie(act1, act2, REL_COOPERATE_WITH, result[2], result[3])


class Mysql:
    def __init__(self, user: str, pwd: str, db: str):
        self.user = user
        self.pwd = pwd
        self.db = db
        self.conn = util.get_mysql_conn(user, pwd, db)
        self.cursor = self.conn.cursor()

    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def get_id(self, table, name):
        self.cursor.execute('select id from ' + table + ' where name=%s ;', (name,))
        return self.cursor.fetchone()

    def get_results(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for each in results:
            yield each


class Neo4j:
    def __init__(self, pwd: str, url="http://localhost:7474"):
        self.pwd = pwd
        self.url = url
        self.graph = util.get_neo4j_graph(pwd, url)

    def find_node_by_name(self, label, name):
        return self.graph.nodes.match(label).where(name=name).first()

    def insert_one_movie(self, node1, node2, rel_type, movie_id, movie_name):
        # 关系
        if node1 and node2:
            rel = self.graph.match((node1, node2), rel_type).first()
            if rel:
                # 若存在关系，则添加电影名，电影名/id不能重复
                if movie_id not in rel[MOVIE_ID]:
                    rel[MOVIE_ID].append(movie_id)
                    rel[MOVIE_NAME].append(movie_name)
                    self.graph.push(rel)
                return
            else:
                # 若不存在，则新建一个关系
                rel = Relationship(node1, rel_type, node2)
                rel[MOVIE_ID] = [movie_id]
                rel[MOVIE_NAME] = [movie_name]
                self.graph.create(rel)
                return

