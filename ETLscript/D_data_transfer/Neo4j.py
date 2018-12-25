import util
from py2neo.data import Relationship

# neo4j
MOVIE_ID = 'movie_id'
MOVIE_NAME = 'movie_name'
LABEL_ACTOR = 'actor'
LABEL_DIRECTOR = 'director'
REL_WORK_WITH = 'work_with'
REL_COOPERATE_WITH = 'cooperate_with'


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