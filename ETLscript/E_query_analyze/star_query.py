from D_data_transfer.MySQLManager import *
from config import  *

time1 = 'select day_of_week, count(1) c from time group by day_of_week order by c desc'
time2 = 'select year, count(1) c from time group by year order by c desc;'
time3 = 'select year, month, count(1) c from time where month>0 group by year, month order by c desc;'

movie1 = 'select name, review, version_count vc from movie where review>100 order by vc desc, review desc;'
movie2 = 'select avg(star) avg_star, rated from movie group by rated order by avg_star desc;'
movie0 = 'select star, avg(review) from movie group by star order by star desc;'

director1 = 'select name, count(1) from director where name=\'Jim Edward\';'
director2 = 'select name, count(1) c from director group by name order by c desc;'
director0 = 'select movie_name from director where name=\'Jim Edward\';'

actor1 = 'select name, count(1) from actor where name=\'Wendy Braun\';'
actor2 = 'select name, count(1) c from actor group by name order by c desc;'

genre1 = 'select name, count(1) from genre where name=\'Action\';'
genre2 = 'select name, count(1) c from genre group by name order by c desc;'

studio1 = 'select name, count(1) from studio where name=\'CreateSpace\';'
studio2 = 'select name, count(1) c from studio group by name order by c desc;'

comb1 = 'select actor_name1, actor_name2, count(1) c from cooperate_with group by actor_name1, actor_name2 order by c desc limit 100;'
comb21 = 'select actor_name1, actor_name2, coop_times from coop_rank order by coop_times desc limit 100;'
comb22 = 'select movie.name, movie.star from movie, cooperate_with cw where cw.actor_name1=\'Moe Howard\' and cw.actor_name2=\'Curly Howard\' and cw.movie_name=movie.name order by movie.star desc;'
comb23 = 'with movies as (select movie_name from cooperate_with where actor_name1=\'Moe Howard\' and actor_name2=\'Curly Howard\')select movie_name, star from movies, movie where movies.movie_name=movie.name order by star desc;'
comb24 = 'select movie_name from cooperate_with where actor_name1=\'Moe Howard\' and actor_name2=\'Curly Howard\';'
comb31 = 'select actor_name, director_name, count(1) c from work_with group by actor_name, director_name order by c desc;'
comb32 = 'select actor_name, director_name, work_times from work_rank order by work_times desc limit 100;'
comb4 = 'with movies as (select movie_name from director where name=\'Cerebellum Corporation\') select avg(star) avg_star, avg(review) avg_review from movies, movie where movies.movie_name=movie.name order by avg_star desc, avg_review desc;'
comb51 = 'select actor.movie_name, time.day_of_week from time, actor where actor.name=\'Jackie Chan\' and actor.movie_name=time.movie_name order by day_of_week;'
comb52 = 'with movies as (select movie_name from actor where name=\'Jackie Chan\') select time.movie_name, day_of_week from movies, time where movies.movie_name=time.movie_name order by day_of_week;'
comb6 = 'select director.name, studio.name, count(1) c from director, studio where director.movie_name=studio.movie_name group by director.name, studio.name order by c desc;'
comb71 = 'select movie.name, genre.name, movie.review, time.year, time.month, time.day from movie, genre, time where genre.name=\'Action\' and genre.movie_name=movie.name=time.movie_name order by movie.review desc limit 100; '
comb72 = 'with movies as (select movie.name movie_name, movie.review review, genre.name genre from movie, genre where movie.name=genre.movie_name and genre.name=\'Action\' order by review desc limit 100) select movies.movie_name, movies.review, movies.genre, time.year, time.month, time.day from movies, time where time.movie_name=movies.movie_name;'


def query(sql, loop):
    mysql = MysqlManager(MYSQL_USR, MYSQL_PWD, MYSQL_DB)
    time_1 = datetime.datetime.now()

    for i in range(loop):
        results = mysql.execute_query(sql)
        for record in results:
            print(record)
            pass

    time_2 = datetime.datetime.now()
    mysql.close_connection()
    print(time_2-time_1)


def star_query_main():
    loop = 1
    query(movie0, loop)


if __name__ == '__main__':
    star_query_main()