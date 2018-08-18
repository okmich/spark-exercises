////////////////////////////
/////// Get number of movies for each Genres - Genres, count
////////////////////////////

//load the data - dataframe
val df = sc.textFile("/user/maria_dev/rawdata/movielens/1m/movies").
	map((s: String) => {
		val fields = s.split("::")
		(fields(0), fields(1), fields(2))
	}).toDF("id", "title", "genres")

// transform
import org.apache.spark.sql.functions._

// val first = df.select("genres")
// val second  = first.select(split($"genres", "\\|").as("genres"))
// val third = second.select(explode($"genres").as("genre"))
// val fourth = third.groupBy("genre")
// val genreDF = fourth.count

val genreDF = df.select(explode(split($"genres", "\\|")).as("genre")).
	groupBy("genre").
	count

// save or print
genreDF.show
genreDF.write.saveAsTable("db.tableName")