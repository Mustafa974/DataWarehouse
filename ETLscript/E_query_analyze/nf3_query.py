import datetime

from D_data_transfer.MySQLManager import *

# mysql
USER = 'amazon'
PASSWORD = 'amazon'
DATABASE = 'amazon'

movie1 = "select star, avg(review) from movie group by star order by star desc"

time1 = "select day_of_week,count(1) c from movie group by day_of_week order by c desc"
time2 = "select year, name from movie where year=1999"

director1 = "select s_director.name, movie.name from s_movie_director,s_director,movie " \
            "where s_director.id=director_id and movie.id=movie_id and s_director.name='Jim Edward';"
director2 = "select s_director.name,count(1) from s_movie_director,s_director where s_director.id=director_id " \
            "group by s_director.name order by count(1) desc limit 100;"

actor1 = "select s_actor.name, movie.name from s_movie_actor,s_actor,movie " \
         "where s_actor.id=actor_id and movie.id=movie_id and s_actor.name='Wendy Braun';"
actor2 = "select s_actor.name, count(1) from s_movie_actor,s_actor " \
         "where s_actor.id=actor_id group by s_actor.name order by count(1) desc limit 100;"

genre1 = "select s_genre.name, movie.name from s_movie_genre,s_genre,movie " \
         "where s_genre.id=genre_id and movie.id=movie_id and s_genre.name='Action';"
genre2 = "select s_genre.name, count(1) from s_movie_genre,s_genre " \
         "where s_genre.id=genre_id group by s_genre.name order by count(1) desc;"

studio1 = "select s_studio.name, movie.name from s_movie_studio,s_studio,movie " \
          "where s_studio.id=studio_id and movie.id=movie_id and s_studio.name='CreateSpace';"
studio2 = "select s_studio.name, count(1) from s_movie_studio,s_studio " \
          "where s_studio.id=studio_id group by s_studio.name order by count(1) desc limit 100;"

cooperate1 = "select a.name,b.name,count(1) c from s_movie_actor ma,s_movie_actor mb,s_actor a,s_actor b " \
             "where ma.movie_id=mb.movie_id and ma.actor_id<>mb.actor_id and ma.actor_id=a.id and mb.actor_id=b.id " \
             "group by a.name,b.name order by c desc limit 100;"
work1 = "select d.name,a.name,count(1) c from s_movie_director md,s_movie_actor ma,s_director d,s_actor a " \
        "where md.movie_id=ma.movie_id and ma.actor_id=a.id and md.director_id=d.id group by d.name,a.name " \
        "order by c desc limit 100;"
# 组合查询
comb1 = "select m.name,m.star,m.review from s_actor a,s_director d,s_movie_actor ma,s_movie_director md,movie m " \
        "where a.name='Mel Blanc' and d.name='Friz Freleng' and a.id=ma.actor_id and d.id=md.director_id and " \
        "ma.movie_id=m.id and md.movie_id=m.id order by m.star desc,m.review desc;"
comb2 = "select m.name,m.star,m.review from s_actor a,s_actor d,s_movie_actor ma,s_movie_actor md,movie m " \
        "where a.name='Moe Howard' and d.name='Curly Howard' and a.id=ma.actor_id and d.id=md.actor_id and " \
        "ma.movie_id=m.id and md.movie_id=m.id order by m.star desc,m.review desc;"
comb3 = "select d.name,avg(star) avg_star,avg(review) avg_review from s_director d,s_movie_director md,movie m where " \
        "md.director_id=d.id and md.movie_id=m.id group by d.name order by avg_star desc, avg_review desc limit 100;"
comb4 = "select m.name,m.rated,m.star,m.review,m.version_count,m.duration from movie m,s_movie_actor ma,s_actor a " \
        "where a.name='Morgan Freeman' and ma.actor_id=a.id and ma.movie_id=m.id order by m.star desc;"
comb5 = "select month,avg(review) avg_review from movie group by month order by avg_review desc;"
comb6 = "select g.name,avg(m.star) avg_star,avg(m.review) avg_review from s_genre g,s_movie_genre mg,movie m where " \
        "mg.genre_id=g.id and mg.movie_id=m.id group by g.name order by avg_star desc, avg_review desc;"
comb7 = "select s.name,avg(m.review) avg_review from s_studio s,s_movie_studio ms,movie m where ms.studio_id=s.id " \
        "and ms.movie_id=m.id group by s.name order by avg_review desc limit 100;"
# 复杂查询
complex1 = "select m.name, g.name, m.review, m.year, m.month, m.day from movie m,s_genre g,s_movie_genre mg where " \
           "g.name='Action' and mg.movie_id=m.id and mg.genre_id=g.id order by m.review desc limit 100;"
complex2 = "with movies as (select m.id m_id,m.star m_star from s_actor a,s_movie_actor ma,movie m where a.name=" \
           "'Jackie Chan' and ma.actor_id=a.id and ma.movie_id=m.id) select g.name,avg(movies.m_star) avg_star from " \
           "movies,s_genre g,s_movie_genre mg where movies.m_id=mg.movie_id and mg.genre_id=g.id group by g.name " \
           "order by avg_star desc;"
complex3 = "with movies as (select m.id,m.name,m.rated,m.star,m.review,m.version_count,m.duration from s_studio s," \
           "s_movie_studio ms,movie m where s.name='20th Century Fox' and ms.studio_id=s.id and ms.movie_id=m.id) " \
           "select movies.name,movies.rated,movies.star,movies.review,movies.version_count,movies.duration from " \
           "movies,s_genre g,s_movie_genre mg where g.name='Action' and mg.genre_id=g.id and movies.id=mg.movie_id " \
           "order by movies.star desc;"


def nf3_query_main():
    loop = 1000
    query(complex1, loop)


def query(sql, loop=1):
    mysql = MysqlManager(USER, PASSWORD, DATABASE)
    time_1 = datetime.datetime.now()

    for i in range(loop):
        results = mysql.execute_query(sql)
        for record in results:
            # print(i, record)
            # i += 1
            pass
    time_2 = datetime.datetime.now()
    print(time_2-time_1)


