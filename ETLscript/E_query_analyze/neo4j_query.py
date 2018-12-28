from D_data_transfer.Neo4jManager import Neo4jManager
from py2neo import *
import datetime

PASSWORD = 'amazon'
neo4j = Neo4jManager(PASSWORD)

"""
    创建索引：CREATE INDEX ON :movie(name)
    已建索引：所有node的除id以外的属性
"""

movie1 = "MATCH (m:movie) RETURN m.star,avg(m.review) order by m.star DESC"

time1 = "MATCH (m:movie)-[:movie_day_of_week]->(w:day_of_week) RETURN w,count(m) order by count(m) DESC"
time2 = "MATCH (m:movie)-[:movie_year]->(y:year{value:'1999'}) RETURN m"

director1 = "MATCH (m:movie)-[:movie_director]->(d:director{name:'Jim Edward'}) RETURN m"
director2 = "MATCH (m:movie)-[:movie_director]->(d:director) RETURN d.name,count(m) order by count(m) DESC limit 100"

actor1 = "MATCH (m:movie)-[:movie_actor]->(a:actor{name:'Wendy Braun'}) RETURN m"
actor2 = "MATCH (m:movie)-[:movie_actor]->(a:actor) RETURN a.name,count(m) order by count(m) DESC limit 100"

genere1 = "MATCH (m:movie)-[:movie_genre]->(g:genre{name:'Action'}) RETURN m"
genere2 = "MATCH (m:movie)-[:movie_genre]->(g:genre) RETURN g.name,count(m) order by count(m) DESC"

studio1 = "MATCH (m:movie)-[:movie_studio]->(s:studio{name:'CreateSpace'}) RETURN m"
studio2 = "MATCH (m:movie)-[:movie_studio]->(s:studio) RETURN s.name,count(m) order by count(m) DESC limit 100"

cooperate1_1 = "MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_actor]-(b:actor) RETURN a.name,b.name,count(m) " \
             "order by count(m) DESC limit 100"
cooperate1_2 = "MATCH (a)-[r:cooperate_with]->(b) RETURN a.name,b.name,length(r.movie_id) order by length(r.movie_id) "\
             "DESC limit 100"

work1_1 = "MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_director]-(d:director) RETURN a.name,d.name,count(m) " \
           "order by count(m) DESC limit 100"
work1_2 = "MATCH (a:actor)-[r:work_with]-(d:director) RETURN a.name,d.name,length(r.movie_id) " \
          "order by length(r.movie_id) DESC limit 100"

comb1 = "MATCH (a:actor{name:'Mel Blanc'})-[:movie_actor]-(m:movie)-[:movie_director]-(d:director{name:'Friz Freleng'" \
        "}) RETURN m.name,m.star,m.review order by m.star DESC, m.review DESC"
comb2 = "MATCH (a:actor{name:'Moe Howard'})-[:movie_actor]-(m:movie)-[:movie_actor]-(b:actor{name:'Curly Howard'}) " \
        "RETURN a.name,b.name,m.name,m.star order by m.star DESC"
comb3 = "MATCH (m:movie)-[:movie_director]-(d:director) RETURN d.name,avg(m.star),avg(m.review) " \
        "order by avg(m.star) desc, avg(m.review) desc limit 100"
comb4 = "MATCH (:actor{name:'Jackie Chan'})-[:movie_actor]-(m:movie) RETURN m order by m.star desc"
comb5 = "MATCH (m:movie)-[:movie_month]-(mo:month) RETURN mo.value,avg(m.review) order by avg(m.review) desc"
comb6 = "MATCH (m:movie)-[:movie_genre]-(g:genre) RETURN g.name,avg(m.star),avg(m.review) order by " \
        "avg(m.star) desc,avg(m.review) desc"
comb7 = "MATCH (m:movie)-[:movie_studio]-(s:studio) RETURN s.name,avg(m.star) order by avg(m.star) desc limit 100"

complex1 = "MATCH (g:genre{name:'Action'})-[:movie_genre]-(m:movie)-[:movie_year]-(y:year) " \
           "RETURN m.name,m.review,y.value order by m.review DESC limit 100"
complex2 = "MATCH (a:actor{name:'Jackie Chan'})-[:movie_actor]-(m:movie)-[:movie_genre]-(g:genre) " \
           "RETURN g.name,avg(m.star) order by avg(m.star) desc"
complex3 = "MATCH (s:studio{name:'20th Century Fox'})-[:movie_studio]-(m:movie)-[:movie_genre]-(g:genre{name:'Action'" \
           "}) RETURN m order by m.star desc"


def neo4j_query_main():
    query(complex3, 1000)


def query(cql, loop=1):
    tx = Transaction(neo4j.graph)
    i = 0
    time_1 = datetime.datetime.now()
    for i in range(loop):
        cursor = tx.run(cql)
        for rec in cursor:
            # print(i, rec)
        #     i += 1
            pass
    time_2 = datetime.datetime.now()
    print(time_2 - time_1)


