////////////////////////////
/////// Get top N most rated movies
////////////////////////////

//load the data - dataframe
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

movieDF.createOrReplaceTempView("movie")
ratingDF.createOrReplaceTempView("rating")

val nValue = 50
val orderByCol = "avg_rating"

val query = 
s"""
	select m.id, m.title, count(1) as no_ratings, sum(r.rate) as total_rating, avg(r.rate) as avg_rating
	from movie m left join rating r on 
	m.id = r.movieid
	group by m.id, m.title
	order by $orderByCol desc
	limit $nValue
"""

val df = spark.sql(query)

//save or write to table or view on screen
df.show(nValue)
