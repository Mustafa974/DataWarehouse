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

time1 = "MATCH (w:day_of_week)-[r:movie_day_of_week]-() RETURN w,count(r) order by count(r) DESC"
time2 = "MATCH (y:year{value:'1999'})-[:movie_year]-(m:movie) RETURN y.value,m.name"

director1 = "MATCH (d:director{name:'Jim Edward'})-[:movie_director]-(m:movie) RETURN m.name"
director2 = "MATCH (d:director)-[r:movie_director]-() RETURN d.name,count(r) order by count(r) DESC limit 100"

actor1 = "MATCH (a:actor{name:'Wendy Braun'})-[:movie_actor]-(m:movie) RETURN m.name"
actor2 = "MATCH (a:actor)-[r:movie_actor]-() RETURN a.name,count(r) order by count(r) DESC limit 100"

genre1 = "MATCH (g:genre{name:'Action'})-[:movie_genre]-(m:movie) RETURN m.name"
genre2 = "MATCH (g:genre)-[r:movie_genre]-() RETURN g.name,count(r) order by count(r) DESC"

studio1 = "MATCH (s:studio{name:'CreateSpace'})-[:movie_studio]-(m:movie) RETURN m.name"
studio2 = "MATCH (s:studio)-[r:movie_studio]-() RETURN s.name,count(r) order by count(r) DESC limit 100"

cooperate1_1 = "MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_actor]-(b:actor) RETURN a.name,b.name,count(m) " \
             "order by count(m) DESC limit 100"
cooperate1_2 = "MATCH (a)-[r:cooperate_with]->(b) RETURN a.name,b.name,length(r.movie_id) order by length(r.movie_id) "\
             "DESC limit 100"

work1_1 = "MATCH (a:actor)-[:movie_actor]-(m:movie)-[:movie_director]-(d:director) RETURN a.name,d.name,count(m) " \
           "order by count(m) DESC limit 100"
work1_2 = "MATCH (a:actor)-[r:work_with]-(d:director) RETURN a.name,d.name,length(r.movie_id) " \
          "order by length(r.movie_id) DESC limit 100"

comb1 = "MATCH (d:director{name:'Friz Freleng'})-[:movie_director]-(m:movie)-[:movie_actor]-" \
        "(a:actor{name:'Mel Blanc'}) RETURN m.name,m.star,m.review order by m.star DESC, m.review DESC"
comb2 = "MATCH (a:actor{name:'Moe Howard'})-[:movie_actor]-(m:movie)-[:movie_actor]-(b:actor{name:'Curly Howard'}) " \
        "RETURN a.name,b.name,m.name,m.star order by m.star DESC"
comb3 = "MATCH (d:director)-[:movie_director]-(m:movie) RETURN d.name,avg(m.star),avg(m.review) " \
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
complex3 = "MATCH (g:genre{name:'Action'})-[:movie_genre]-(m:movie)-[:movie_studio]-" \
           "(s:studio{name:'20th Century Fox'}) RETURN m order by m.star desc"


def neo4j_query_main():
    query(comb3, 10)


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


