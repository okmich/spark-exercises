val df = spark.read.
	options(Map("header"->"false", "sep"->":", "inferSchema"->"true")).
	csv("/user/maria_dev/rawdata/movielens/1m/movies")

import org.apache.spark.sql.types._
val struct =
StructType(
StructField("id", IntegerType, true) ::
StructField("blank1", StringType, false) ::
StructField("title", StringType, false)  ::
StructField("blank2", StringType, false)  ::
StructField("genres", StringType, false) :: Nil)

val df = spark.read.schema(struct).
	options(Map("header"->"false", "sep"->":")).
	csv("/user/maria_dev/rawdata/movielens/1m/movies")

//use rdd

val df = sc.textFile("/user/maria_dev/rawdata/movielens/1m/movies").
	map((s: String) => {
		val fields = s.split("::")
		(fields(0), fields(1), fields(2))
	}).toDF("id", "title", "genres")


val REGEX = """(.+?)\\s(.+?)$""".r
val vaues = "Toy Story (1995)" match {
case REGEX(title, year) => (title, year)
case all : String => (all,"") 
}
