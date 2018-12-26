from D_data_transfer.Neo4jManager import Neo4jManager
from py2neo import *
import datetime

PASSWORD = 'amazon'
neo4j = Neo4jManager(PASSWORD)

"""
    创建索引：CREATE INDEX ON :movie(name)
    已建索引：所有node的除id以外的属性
"""

time1 = "MATCH (m:movie)-[:movie_day_of_week]->(w:day_of_week) RETURN w,count(m) order by count(m) DESC"
time2 = "MATCH (m:movie)-[:movie_year]->(y:year) RETURN y,count(m) order by count(m) DESC"
time3 = "MATCH (m:month)-[:movie_month]-(mv:movie)-[:movie_year]->(y:year) RETURN y,m,count(mv) order by count(mv) DESC"

movie1 = "MATCH (m:movie) WHERE m.review>100 RETURN m.name,m.review,m.version_count " \
         "order by m.version_count DESC,m.review DESC"

director1 = "MATCH (m:movie)-[:movie_director]->(d:director{name:'Jim Edward'}) RETURN d.name,count(m)"
director2 = "MATCH (m:movie)-[:movie_director]->(d:director) RETURN d.name,count(m) order by count(m) DESC"

actor1 = "MATCH (m:movie)-[:movie_actor]->(a:actor{name:'Wendy Braun'}) RETURN a.name,count(m)"
actor2 = "MATCH (m:movie)-[:movie_actor]->(a:actor) RETURN a.name,count(m) order by count(m) DESC"

genere1 = "MATCH (m:movie)-[:movie_genre]->(g:genre{name:'Action'}) RETURN g.name,count(m)"
genere2 = "MATCH (m:movie)-[:movie_genre]->(g:genre) RETURN g.name,count(m) order by count(m) DESC"

studio1 = "MATCH (m:movie)-[:movie_studio]->(s:studio{name:'CreateSpace'}) RETURN s.name,count(m)"
studio2 = "MATCH (m:movie)-[:movie_studio]->(s:studio) RETURN s.name,count(m) order by count(m) DESC"

complex1 = "MATCH (a)-[r:cooperate_with]->(b) RETURN a.name,b.name,length(r.movie_id) " \
           "order by length(r.movie_id) DESC limit 100"
complex1_2 = "MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_actor]-(b:actor) " \
             "RETURN a.name,b.name,count(m) order by count(m) DESC limit 100"
complex2 = "MATCH (a:actor{name:'Moe Howard'})-[:movie_actor]-(m:movie)-[:movie_actor]-(b:actor{name:'Curly Howard'}) "\
           "RETURN a.name,b.name,m.name,m.star order by m.star DESC"
complex3 = "MATCH (a:actor)-[r:work_with]-(d:director) RETURN a.name,d.name,length(r.movie_id) " \
           "order by length(r.movie_id) DESC"
complex3_2 = "MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_director]-(d:director) " \
             "RETURN a.name,d.name,count(m) order by count(m) DESC"
complex4 = "MATCH (m:movie)-[:movie_director]-(d:director{name:'Cerebellum Corporation'}) " \
           "RETURN d.name,avg(m.star),avg(m.review)"
complex5 = "MATCH (:actor{name:'Jackie Chan'})-[:movie_actor]-(m:movie)-[:movie_day_of_week]-(d:day_of_week) " \
           "RETURN m.name,d.value"
complex6 = "MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_studio]-(s:studio) " \
           "RETURN a.name,s.name,count(m) order by count(m)"
complex7 = "MATCH (g:genre{name:'Action'})-[:movie_genre]-(m:movie)-[:movie_year]-(y:year) " \
           "RETURN m.name,m.review,y.value order by m.review DESC limit 100"


def neo4j_query_main():
    query(complex3_2, 1)


def query(cql, loop=1):
    tx = Transaction(neo4j.graph)
    i = 0
    time_1 = datetime.datetime.now()
    for i in range(loop):
        cursor = tx.run(cql)
        for rec in cursor:
        #     print(E_query_analyze, rec)
        #     i += 1
            pass
    time_2 = datetime.datetime.now()
    print(time_2 - time_1)


