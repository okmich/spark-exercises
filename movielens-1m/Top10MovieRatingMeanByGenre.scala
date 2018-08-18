////////////////////////////
/////// Get top 10 most rated movies by Genres
////////////////////////////
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

val movieDF = sc.textFile("/user/maria_dev/rawdata/movielens/1m/movies").
	map((s: String) => {
		val fields = s.split("::")
		(fields(0), fields(1), fields(2))
	}).toDF("id", "title", "genres")


val ratingDF = sc.textFile("/user/maria_dev/rawdata/movielens/1m/ratings").
	map((s: String) => {
		val fields = s.split("::")
		(fields(0).toInt, fields(1), fields(2), fields(3))
	}).toDF("userid", "movieid", "rate", "ts")

// flat the movie table by genre to create duplicate movie record by genre
val expMovieDF = movieDF.
			withColumn("genre", explode(split($"genres", "\\|"))).
			drop("genres")


expMovieDF.createOrReplaceTempView("movie")
ratingDF.createOrReplaceTempView("rating")

val query = 
s"""
	select genre, title, avg_rating, dense_rank() over (partition by genre order by avg_rating desc) position
	from (
		select m.genre, m.title, count(1) as no_ratings, avg(r.rate) as avg_rating
		from movie m join rating r on 
		m.id = r.movieid
		group by m.genre, m.title
		having count(1) >= 20
	) v
"""

val df = spark.sql(query).
	filter($"position" <= 10).
	select($"genre", $"title", $"avg_rating", $"position").
	groupBy($"genre").
	pivot("position").
	agg(min(concat($"title", lit(" - ").cast(StringType), $"avg_rating".cast(StringType))))

