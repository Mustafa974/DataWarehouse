from D_data_transfer.MySQLManager import *
from config import *

time1 = 'select day_of_week, count(1) c from time group by day_of_week order by c desc'
time2 = 'select year, count(1) c from time group by year order by c desc;'
time3 = 'select year, month, count(1) c from time where month>0 group by year, month order by c desc;'

movie1 = 'select name, review, version_count vc from movie where review>100 order by vc desc, review desc;'
movie2 = 'select avg(star) avg_star, rated from movie group by rated order by avg_star desc;'

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


# 增加索引、修改查询语句后：
movie01 = 'select star, avg(review) from movie group by star order by star desc;'
time01 = 'select day_of_week, count(1) c from time group by day_of_week order by c desc;'
time02 = 'select year, movie_name from time where year=1999;'
director01 = 'select name, movie_name from director where name=\'Jim Edward\';'
director02 = 'select name, count(1) c from director group by name order by c desc limit 100;'
actor01 = 'select name, movie_name from actor where name=\'Wendy Braun\';'
actor02 = 'select name, count(1) c from actor group by name order by c desc limit 100;'
genre01 = 'select name, movie_name from genre where name=\'Action\';'
genre02 = 'select name, count(1) c from genre group by name order by c desc;'
studio01 = 'select name, movie_name from studio where name=\'CreateSpace\';'
studio02 = 'select name, count(1) c from studio group by name order by c desc limit 100;'
coop01 = 'select actor_name1, actor_name2, count(1) c from cooperate_with group by actor_name1, actor_name2 order by c desc limit 100;'
work01 = 'select actor_name, director_name, count(1) c from work_with group by actor_name, director_name order by c desc limit 100;'
cq01 = 'with movies as (select movie_id, movie_name from work_with where actor_name=\'Mel Blanc\' and director_name=\'Friz Freleng\') select movie.name, movie.star, movie.review from movie, movies where movie.id=movies.movie_id order by movie.star desc, movie.review desc;'
cq02 = 'with movies as (select movie_id, movie_name from cooperate_with where actor_name1=\'Moe Howard\' and actor_name2=\'Curly Howard\') select movie_name, star, review from movies, movie where movies.movie_id=movie.id order by star desc, review desc;'
cq03 = 'select director.name, avg(star) avg_star, avg(review) avg_review from movie, director where movie.id=director.movie_id group by director.name order by avg_star desc, avg_review desc limit 100;'
cq04 = 'select movie.name, movie.rated, movie.star, movie.review, movie.version_count, movie.duration from movie, actor where actor.name=\'Morgan Freeman\' and actor.movie_id=movie.id order by star desc;'
cq05 = 'select month, avg(review) avg_review from movie, time where movie.id=time.movie_id group by month order by avg_review desc;'
cq06 = 'select genre.name type, avg(movie.star) avg_star, avg(movie.review) avg_review from movie, genre where genre.movie_id=movie.id group by genre.name order by avg_star desc, avg_review desc;'
cq07 = 'select studio.name studio, avg(movie.review) avg_review from movie, studio where studio.movie_id = movie.id group by studio.name order by avg_review desc limit 100;'
complex01 = "with movies as (select movie.id movie_id, movie.name movie_name, movie.review review, genre.name genre from movie, genre where movie.id=genre.movie_id and genre.name='Action' order by review desc limit 100) select movies.movie_name, movies.review, movies.genre, time.year, time.month, time.day from movies, time where time.movie_id=movies.movie_id;"
complex02 = "with movies as (select movie.id movie_id, movie.name movie_name, movie.star star from movie, actor where actor.name='Jackie Chan' and actor.movie_id=movie.id) select genre.name genre, avg(movies.star) avg_star from movies, genre where movies.movie_id=genre.movie_id group by genre.name order by avg_star desc;"
complex03 = "with movies as (select studio.movie_id from studio, genre where studio.name='20th Century Fox' and genre.name='Action' and studio.movie_id=genre.movie_id) select movie.name, movie.rated, movie.star, movie.review, movie.version_count, movie.duration from movie, movies where movie.id=movies.movie_id order by movie.star desc;"


def query(sql, loop):
    mysql = MysqlManager(MYSQL_USR, MYSQL_PWD, MYSQL_DB)
    time_1 = datetime.datetime.now()

    for i in range(loop):
        results = mysql.execute_query(sql)
        for record in results:
            # print(record)
            pass

    time_2 = datetime.datetime.now()
    mysql.close_connection()
    print(time_2-time_1)


def star_query_main():
    loop = 100
    query(complex03, loop)


if __name__ == '__main__':
    star_query_main()


