
loadDataToMovie = 'load data infile  "/Users/mustafa/Desktop/movie.txt" ignore into table AmazonMovie.movie ' \
                  'character set utf8 fields terminated by \'，\' (url, name, rated, type, star, duration, review, version_count);'
loadDataToGenre = 'load data infile "/Users/mustafa/Desktop/genre.txt"  into table AmazonMovie.genre ' \
                  'character set utf8 fields terminated by \'，\' (name, movie_id, movie_name);'
loadDataToStudio = 'load data infile "/Users/mustafa/Desktop/studio.txt"  into table AmazonMovie.studio ' \
                   'character set utf8 fields terminated by \'，\' (name, movie_id, movie_name);'
loadDataToActor = 'load data infile "/Users/mustafa/Desktop/actor.txt"  into table AmazonMovie.actor ' \
                  'character set utf8 fields terminated by \'，\' (name, movie_id, movie_name);'
loadDataToDirector = 'load data infile "/Users/mustafa/Desktop/director.txt"  into table AmazonMovie.director ' \
                  'character set utf8 fields terminated by \'，\' (name, movie_id, movie_name);'
loadDataToTime = 'load data infile "/Users/mustafa/Desktop/time.txt"  into table AmazonMovie.time ' \
                 'character set utf8 fields terminated by \'，\' (year, month, day, day_of_weed, movie_id, movie_name);'
loadDataToCoop = 'load data infile "/Users/mustafa/Desktop/coop.txt"  into table AmazonMovie.cooperate_with ' \
                 'character set utf8 fields terminated by \'，\' (actor_name1, actor_name2, movie_id, movie_name);'
loadDataToWork = 'load data infile "/Users/mustafa/Desktop/work.txt"  into table AmazonMovie.work_with ' \
                 'character set utf8 fields terminated by \'，\' (actor_name, director_name, movie_id, movie_name);'
