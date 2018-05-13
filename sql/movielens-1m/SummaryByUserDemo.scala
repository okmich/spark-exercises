////////////////////////////
/////// Get the full rating summaries for each combination of user group in exercise 2. - 
/////// Gender, Age, Occupation, MovieTitle, Rating, event_year, event_month, event_day 
/////// order by event_year, event_month, event_day desc
////////////////////////////
import org.apache.spark.sql.functions._

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

val userDF = sc.textFile("/user/maria_dev/rawdata/movielens/1m/users").
	map((s: String) => {
		val fields = s.split("::")
		(fields(0).toInt, fields(1), fields(2), fields(3), fields(4))
	}).toDF("uid", "gender", "age", "occupation", "zip")


val dataDF = ratingDF.join(movieDF, $"movieid" === $"id").
	join(userDF, $"userid" === $"uid").
	select($"gender", $"age", $"occupation", $"title".as("movietitle"), 
		$"rate", year(from_unixtime($"ts")).as("event_year"), 
		month(from_unixtime($"ts")).as("event_month"), 
		dayofmonth(from_unixtime($"ts")).as("event_day"), 
		date_format(from_unixtime($"ts"), "EEEEEE").as("event_dayofweek")
	)

dataDF.write.saveAsTable("full_rating_summary_2")