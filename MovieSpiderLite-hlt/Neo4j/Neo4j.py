from py2neo import Relationship, Node, Graph
from Config import Config
from Managers import MongoManager

DEFAULT_LABEL = 'Default'
ACTOR_LABEL = 'Actor'
DIRECTOR_LABEL = 'Director'
MOVIE_LABEL = 'Movie'

DIRECT_LABEL = 'Direct'
ACTIN_LABEL = 'ActIn'
COOPERATE_LABEL = 'CooperateWith'


class Neo4j(object):

    def __init__(self):
        self.graph = Graph(Config.NEO_URL, username=Config.NEO_USR, password=Config.NEO_PSW)
        self.mm = MongoManager.DBManager()

    def add_relation(self, node_name1, node_name2, movie_name='name', url='url'):
        """
        图中添加新的导演关系
        若关系中的两个节点不在图中则自动创建
        同时为关系添加电影名、发行时间、关系计数这几个参数
        :param node_name1:
        :param node_name2:
        :param movie_name:
        :param release_time:
        :return:
        """
        node1 = Node(DIRECTOR_LABEL, name=node_name1)
        node1['type'] = 'director'
        node2 = Node(ACTOR_LABEL, name=node_name2)
        # node3 = Node(MOVIE_LABEL, name=movie_name)
        # node3['url'] = url
        #
        # actor_movie_relation = Relationship(node2, ACTIN_LABEL, node3)
        # director_movie_relation = Relationship(node1, DIRECT_LABEL, node3)
        # self.graph.merge(actor_movie_relation, DEFAULT_LABEL, 'name')
        # self.graph.merge(director_movie_relation, DEFAULT_LABEL, 'name')

        # print(actor_movie_relation)
        # print(director_movie_relation)

        # if self.find_relation(node_name1, node_name2):
        #     print('relation already existed, add count')
        # else:
        relation = Relationship(node1, COOPERATE_LABEL, node2)
        relation['count'] = 1
        self.graph.merge(relation, DEFAULT_LABEL, 'name')
        # print("成功创建关系", node_name1, ',', COOPERATE_LABEL, ',', node_name2)

    def print(self, name, relation):
        """
        打印所有以名字为name的节点开始、具有relation关系的边的终节点的信息
        :param name:
        :param relation:
        :return:
        """
        print('##########')
        query = 'MATCH (n) WHERE n.name={name} RETURN n'
        params = dict(name=name)
        node = self.graph.evaluate(query, params)
        print(node)
        for rel in self.graph.match((node,), relation):
            print(rel.end_node['name'], rel.end_node.labels, rel['movie_name'], rel['release_time'])

    def find_director_node(self, name):
        """
        查找具有某名字的节点，若图中有此节点则返回true，反之返回false
        :param name:
        :return:
        """
        query = 'MATCH (n:Director) WHERE n.name={name} RETURN n'
        params = dict(name=name)
        node = self.graph.evaluate(query, params)
        if node is None:
            return False
        if self.graph.exists(node):
            return True
        else:
            return False

    def find_actor_node(self, name):
        """
        查找具有某名字的节点，若图中有此节点则返回true，反之返回false
        :param name:
        :return:
        """
        query = 'MATCH (n:Actor) WHERE n.name={name} RETURN n'
        params = dict(name=name)
        node = self.graph.evaluate(query, params)
        if node is None:
            return False
        if self.graph.exists(node):
            return True
        else:
            return False

    def get_labeled_node(self, count=1):
        """
        获取具有某个标签的节点列表
        打印节点数量
        并返回该list
        :return:
        """
        # 用CQL进行查询，返回的结果是list
        datas = self.graph.data('MATCH(p:Director) return p')
        # 目标节点数量
        # print(len(datas))
        # 数据类型为list
        # print(type(datas))
        _count = 1
        for data in datas:
            # data类型为dict
            # print(type(data))
            # if _count > count:
            #     break
            print(data)
            _count += 1
        print('Total count of Director is', _count)
        return datas

    def find_relation_and_add_count(self, name1, name2):
        """
        查找分别以name1, name2为起始、终止节点的 CooperateWith 关系
        若找到则对应count数加一
        :param name1:
        :param name2:
        :return:
        """
        sn = self.graph.find_one(DIRECTOR_LABEL, property_key='name', property_value=name1)
        en = self.graph.find_one(ACTOR_LABEL, property_key='name', property_value=name2)
        rel = self.graph.match(start_node=sn, rel_type=COOPERATE_LABEL, end_node=en)
        # print(rel)

        # print('--------')
        query = 'MATCH(n:Director)-[r:CooperateWith]->(m:Actor) WHERE n.name={name1} and m.name={name2} RETURN r'
        params = dict(name1=name1, name2=name2)
        relation = self.graph.evaluate(query, params)
        if relation is None:
            print('relation is none')
            self.add_relation(name1, name2)
            return False
        if self.graph.exists(relation):
            print('relation exists, add count')
            relation['count'] += 1
            self.graph.push(relation)
            print(relation.start_node()['name'], '->', relation['count'], '->', relation.end_node()['name'])
            return True
        else:
            print('relation does not exist')
            return False

    def clear_graph(self):
        """
        清空图数据库
        :return:
        """
        self.graph.delete_all()

    def show_end_node(self, name, relation_label):
        """
        根据输入的起始节点名和关系标签，遍历全部对应关系，并打印终节点的属性群
        :param name:
        :param relation_label:
        :param attrs:
        :return:
        """
        query = 'MATCH (n) WHERE n.name={name} RETURN n'
        params = dict(name=name)
        node = self.graph.evaluate(query, params)
        if node is None:
            print('node is None!')
            return False
        if self.graph.exists(node):
            print(node)
            # 遍历此起始节点的全部关系，打印关系的个数
            for rel in self.graph.match((node,), relation_label):
                print(name, '->', rel['count'], '->', rel.end_node['name'])
        else:
            print('node not exists!')
            return False

    def get_coop_count(self):
        """
        获取全部导演、演员合作关系及次数并打印
        :return:
        """
        directors = self.get_labeled_node()
        # print(type(directors))
        count = 1
        for director in directors:
            if count > 1:
                break
            # print(director['p']['name'])
            self.show_end_node(director['p']['name'], COOPERATE_LABEL)
            count += 1

    def get_cooperations(self):
        directors = self.get_labeled_node()
        # datas = []
        for director in directors:
            query = 'MATCH (n) WHERE n.name={name} RETURN n'
            params = dict(name=director['p']['name'])
            node = self.graph.evaluate(query, params)
            if node is None:
                print('node is None!')
                return None
            if self.graph.exists(node):
                # 遍历此起始节点的全部关系，一一存入结果集并返回
                for rel in self.graph.match(start_node=node, rel_type=COOPERATE_LABEL):
                    data = {
                        'director': director['p']['name'],
                        'actor': rel.end_node()['name'],
                        'count': rel['count']
                    }
                    # print("合作信息，", data)
                    self.mm.save_data(Config.COOPERATION_TEMP, data)
                    # datas.append(data)
            else:
                print('node not exists!')
                return None
        # return datas


def main():

    neo = Neo4j()

    # neo.clear_graph()

    # print('###')
    # neo.add_relation('director1', 'actor1')
    # print('###')
    # neo.add_relation('director1', 'actor2')
    # print('###')
    # neo.add_relation('director1', 'actor3')
    # print('###')
    # neo.add_relation('director1', 'actor4')
    # print('###')
    # neo.add_relation('director1', 'actor5')
    # print('###')
    # neo.add_relation('director2', 'actor2')
    # print('###')
    # neo.add_relation('director3', 'actor3')
    # print('###')
    # neo.add_relation('director4', 'actor4')
    # print('###')
    # neo.add_relation('director5', 'actor5')
    # print('###')
    # neo.add_relation('director6', 'actor6')
    # print('###')
    # neo.add_relation('director7', 'actor7')
    # print('###')
    # neo.add_relation('director8', 'actor8')
    # print('###')
    # neo.add_relation('director9', 'actor9')
    # print('###')
    # neo.add_relation('director10', 'actor10')
    # print('###')
    # neo.find_relation_and_add_count('director1', 'actor2')
    # print('###')
    # neo.find_relation_and_add_count('director1', 'actor3')
    # print('###')
    # neo.find_relation_and_add_count('director1', 'actor4')
    # print('###')
    # neo.find_relation_and_add_count('director1', 'actor5')

    # neo.add_relation('director11', 'actor11', 'movie11', '12')
    # neo.add_relation('director12', 'actor12', 'movie12', '13')
    # neo.add_relation('director13', 'actor13', 'movie13', '11')
    # neo.add_relation('director14', 'actor14', 'movie14', '12')
    # neo.add_relation('director15', 'actor15', 'movie15', '13')
    # neo.print('director2', RELATION_LABEL)
    # attrs = ['name', 'url']
    # neo.show_end_node('director15', COOPERATE_LABEL, attrs)

    # print('###')

    # neo.get_labeled_node()
    neo.get_coop_count()



if __name__ == '__main__':
    main()