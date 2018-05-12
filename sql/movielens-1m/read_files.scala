val df = spark.read.options(Map("header"->"false", "sep"->":", "inferSchema"->"true")).csv("/user/maria_dev/rawdata/movielens/1m/movies")

import org.apache.spark.sql.types._
val struct =
StructType(
StructField("id", IntegerType, true) ::
StructField("blank1", StringType, false) ::
StructField("title", StringType, false)  ::
StructField("blank2", StringType, false)  ::
StructField("genres", StringType, false) :: Nil)