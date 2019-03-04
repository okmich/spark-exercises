spark-shell --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 --conf spark.cassandra.connection.host=192.168.8.100
import org.apache.spark.sql.cassandra._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val itemGroupDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/lookup/item_group")

val testTypeDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/lookup/mdr_test_types")

val testOutcomeDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/lookup/mdr_test_outcome")

val fuelTypeDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/lookup/mdr_fuel_types")

val itemLocationDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/lookup/mdr_rfr_location")





def getSchemaFromFile(file: String) : StructType = {
import scala.io.Source
val json = Source.fromFile(file).getLines.next
DataType.fromJson(json).asInstanceOf[StructType]
}

val testItemDF = spark.read.option("header", "true").option("sep", "|").schema(getSchemaFromFile("testitem.json")).csv("/user/maria_dev/mot_data/test_item/f2005")
val testResultDF = spark.read.option("header", "true").option("sep", "|").schema(getSchemaFromFile("testresult.json")).csv("/user/maria_dev/mot_data/test_result/f2005")
val testDetailsDF = spark.read.option("header", "true").option("sep", "|").schema(getSchemaFromFile("testdetail.json")).csv("/user/maria_dev/mot_data/lookup/item_details")

def getTestDetalsWithItemGroupHierarchy(spark: SparkSession, 
detailsDF: DataFrame, 
itemGroupDF: DataFrame) : DataFrame = {
import spark.implicits._
val baseDF = itemGroupDF.cache
val level1DF = baseDF.as("level1DF")
val level2DF = baseDF.as("level2DF")
val level3DF = baseDF.as("level3DF")
val level4DF = baseDF.as("level4DF")

baseDF.join(level1DF, (baseDF("parent_id") === $"level1DF.test_item_id") 
&& (baseDF("test_class_id") === $"level1DF.test_class_id"), "left_outer").
join(level2DF, $"level1DF.parent_id" === $"level2DF.test_item_id" 
&& $"level1DF.test_class_id" === $"level2DF.test_class_id", "left_outer").
join(level3DF, $"level2DF.parent_id" === $"level3DF.test_item_id" 
&& $"level2DF.test_class_id" === $"level3DF.test_class_id", "left_outer").
join(level4DF, $"level3DF.parent_id" === $"level4DF.test_item_id" 
&& $"level3DF.test_class_id" === $"level4DF.test_class_id", "left_outer").
join(detailsDF, detailsDF("test_item_id") === baseDF("test_item_id") && 
detailsDF("test_class_id") === baseDF("test_class_id")).
select(baseDF("test_class_id"), $"rfr_id",
baseDF("test_item_id"), 
$"minor_item", $"rfr_desc", $"rfr_loc_marker", 
$"rfr_insp_manual_desc", 
$"rfr_advisory_text", 
detailsDF("test_item_set_section_id"),
baseDF("item_name").as("item_group_level_1"), 
$"level1DF.item_name".as("item_group_level_2"),
$"level2DF.item_name".as("item_group_level_3"),
$"level3DF.item_name".as("item_group_level_4"),
$"level4DF.item_name".as("item_group_level_5"))
}

val df1 = getTestDetalsWithItemGroupHierarchy(spark, testDetailsDF, itemGroupDF)
df1.write.cassandraFormat("test_item_details", "movielens").save()

def generateTestResultModel(spark: SparkSession, 
resultDF: DataFrame, fuelTypeDF: DataFrame,
testTypeDF: DataFrame, outcomeDF: DataFrame) : DataFrame = {
import spark.implicits._

resultDF.join(fuelTypeDF, resultDF("fuel_type") === fuelTypeDF("type_code"), "left_outer").
join(testTypeDF, resultDF("test_type") === testTypeDF("type_code"), "left_outer").
join(outcomeDF, resultDF("test_result") === outcomeDF("result_code"), "left_outer").
select(resultDF("test_id"), $"vehicle_id", $"test_date".cast(DateType), 
resultDF("test_type").as("test_type_code"),
resultDF("test_result").as("test_result_code"), $"test_class_id", 
$"test_mileage", $"postcode_area", $"make", $"model", $"cylinder_capacity", 
$"first_use_date".cast(DateType), testTypeDF("test_type"), 
outcomeDF("result").as("test_result"),
resultDF("fuel_type").as("fuel_type_code"), fuelTypeDF("fuel_type"))
}

val df2 = generateTestResultModel(spark, testResultDF, fuelTypeDF, testTypeDF, testOutcomeDF)




val itemGroupDF = spark.
read.
format("org.apache.spark.sql.cassandra").
options(Map(
"table" -> "test_item_details",
"keyspace" -> "movielens")
).load()



