////////////////////////////
/////// Get number of movies for each Genres - Genres, count
////////////////////////////

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val lookupStruct =
StructType(StructField("code", StringType, true) :: StructField("description", StringType, false) :: Nil)

case class User(id: Int, gender: String, age: String, occupation: String, zip: String) extends java.io.Serializable
case class LookupItem(code: String, description: String) extends java.io.Serializable

//load the data - dataframe
val ds = sc.textFile("/user/maria_dev/rawdata/movielens/1m/users").
	map((s: String) => {
		val fields = s.split("::")
		(fields(0).toInt, fields(1), fields(2), fields(3), fields(4))
	}).toDF("id", "gender", "age", "occupation", "zip").as[User]

//load age and occupation look up values
val ageDS = spark.read.option("sep", ":").schema(lookupStruct).
csv("/user/maria_dev/rawdata/movielens/1m/age").as[LookupItem]
val occupDS = spark.read.option("sep", ":").schema(lookupStruct).
csv("/user/maria_dev/rawdata/movielens/1m/occupation").as[LookupItem]

// transform
// join the main table with the other two lookup tables
val wholeDS = ds.join(ageDS, $"age" === ageDS("code"), "left_outer").
				join(occupDS, $"occupation" === occupDS("code"), "left_outer").
				select(when($"gender" === "F", lit("Female")).otherwise("Male").cast(StringType).as("gender"), 
					ageDS("description").as("age"), 
					occupDS("description").as("occupation"))

val distDS = wholeDS.groupBy($"gender",$"age",$"occupation").count

//save to a table
distDS.write.saveAsTable("user_demographic_summary")