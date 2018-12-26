import datetime

from D_data_transfer.import_data_neo4j import Mysql

# mysql
USER = 'amazon'
PASSWORD = 'amazon'
DATABASE = 'amazon'

time1 = "select day_of_week,count(1) c from movie where day_of_week>0 group by day_of_week order by c desc"
time2 = "select year,count(1) c from movie where year>0 group by year order by c desc"
time3 = "select year,month,count(1) from movie where month>0 group by year,month order by year,month;"

movie1 = "select name, review, version_count vc from movie where review>100 order by vc desc, review desc;"
movie2 = "select avg(star) avg_star, rated from movie group by rated order by avg_star desc;"

director1 = "select s_director.name, count(1) from s_movie_director,s_director " \
            "where s_director.id=director_id and s_director.name=\'Jim Edward\' group by s_director.name;"
director2 = "select s_director.name,count(1) from s_movie_director,s_director where s_director.id=director_id " \
            "group by s_director.name order by count(1) desc;"

actor1 = "select s_actor.name, count(1) from s_movie_actor,s_actor " \
         "where s_actor.id=actor_id and s_actor.name=\'Wendy Braun\' group by s_actor.name;"
actor2 = "select s_actor.name, count(1) from s_movie_actor,s_actor " \
         "where s_actor.id=actor_id group by s_actor.name order by count(1) desc;"

genre1 = "select s_genre.name, count(1) from s_movie_genre,s_genre " \
         "where s_genre.id=genre_id and s_genre.name=\'Action\' group by s_genre.name;"
genre2 = "select s_genre.name, count(1) from s_movie_genre,s_genre " \
         "where s_genre.id=genre_id group by s_genre.name order by count(1) desc;"

studio1 = "select s_studio.name, count(1) from s_movie_studio,s_studio " \
          "where s_studio.id=studio_id and s_studio.name='CreateSpace' group by s_studio.name;"
studio2 = "select s_studio.name, count(1) from s_movie_studio,s_studio " \
          "where s_studio.id=studio_id group by s_studio.name order by count(1) desc;"


def nf3_query_main():
    loop = 100
    query(studio2, loop)


def query(sql, loop=1):
    mysql = Mysql(USER, PASSWORD, DATABASE)
    time_1 = datetime.datetime.now()

    for i in range(loop):
        mysql.cursor.execute(sql)
        results = mysql.cursor.fetchall()
        for record in results:
            # print(record)
            pass

    time_2 = datetime.datetime.now()
    print(time_2-time_1)


