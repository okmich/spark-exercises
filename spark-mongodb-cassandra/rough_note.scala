spark-shell --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.0 --conf "spark.mongodb.input.uri=mongodb://192.168.8.102/movielens.users?readPreference=primaryPreferred" --conf "spark.mongodb.output.uri=mongodb://192.168.8.102/movielens.users"

val testItemDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/test_item")

val testResultDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/test_result")

val testDetailsDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/item_details")

val itemGroupDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/item_group")

val testTypeDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/mdr_test_types")

val testOutcomeDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/mdr_test_outcome")

val fuelTypeDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/mdr_fuel_types")

val itemLocationDF = spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).csv("/user/maria_dev/mot_data/mdr_rfr_location")


scala> testDetailsDF.count
res27: Long = 12595

scala> testResultDF.count
res28: Long = 7499745

scala> testItemDF.count
res29: Long = 9844490

scala> itemGroupDF.count
res30: Long = 2138

scala> testTypeDF.count
res31: Long = 5

scala> testOutcomeDF.count
res36: Long = 7

scala> fuelTypeDF.count
res33: Long = 14

scala> itemLocationDF.count
res35: Long = 129



import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.bson._

def getSchemaFromFile(file: String) : StructType = {
import scala.io.Source
val json = Source.fromFile(file).getLines.next
DataType.fromJson(json).asInstanceOf[StructType]
}

val testItemDF = spark.read.option("header", "true").option("sep", "|").schema(getSchemaFromFile("testitem.json")).csv("/user/maria_dev/mot_data/test_item")
val testResultDF = spark.read.option("header", "true").option("sep", "|").schema(getSchemaFromFile("testresult.json")).csv("/user/maria_dev/mot_data/test_result")
val testDetailsDF = spark.read.option("header", "true").option("sep", "|").schema(getSchemaFromFile("testdetail.json")).csv("/user/maria_dev/mot_data/item_details")




//converting dataframe to Document for inserting to MongoDB


import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.bson._

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray


def rowToDocument(row: Row): Document = {
  def conform(sf: StructField, obj: Any) : Any = {
    sf.dataType match {
      case _: BooleanType => (if (obj == null) null else new java.lang.Boolean(obj.toString()))
      case _: DateType => (if (obj == null) null else new BsonDateTime((obj.asInstanceOf[java.sql.Date]).getTime))
      case _: DecimalType => (if (obj == null) null else obj.toString().toDouble)
      case _: DoubleType | FloatType => (if (obj == null) null else obj.toString().toDouble)
      case _: IntegerType => (if (obj == null) null else obj.toString().toInt)
      case _: LongType | ShortType => (if (obj == null) null else obj.toString().toLong)
      case _: StringType => (if (obj == null) null else obj.toString())
      case _: TimestampType => (if (obj == null) null else new BsonDateTime((obj.asInstanceOf[java.sql.Timestamp]).getTime))
      case _: VarcharType => (if (obj == null) null else obj.toString())
      case _: StructType =>  (if (obj == null) null else rowToDocument(obj.asInstanceOf[Row]))
      case e: ArrayType => {
        val innerValues = (obj.asInstanceOf[WrappedArray[Any]]).map((o : Any) => if (o == null) null else conform(new StructField("", e.elementType), o))
        innerValues.asJava
      }
      case _ => null
    }
  }

  val doc = new Document()
  val valuesAndTypes : Seq[(Any, StructField)] = row.toSeq.zip(row.schema)
  valuesAndTypes.foreach((t: (Any, StructField)) => doc.put(t._2.name, simpleFieldConformance(t._2, t._1)))
  doc
}

val docsRDD = itemGroupDF.rdd.map(rowToDocument)



or try



MongoSpark.save(df.write.option("collection", "collection_name").mode("save mode"))


spark-shell --conf "spark.mongodb.input.uri=mongodb://192.168.8.101/wakapoll.response" \
                  --conf "spark.mongodb.output.uri=mongodb://192.168.8.101/wakapoll.response" \
                  --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0

import com.mongodb.spark._
import com.mongodb.spark.config._

val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
val writeConfig = WriteConfig("mot", "item_group", "mongodb://192.168.8.102/mot", 10000, WriteConcern.MAJORITY)
MongoSpark.save(itemGroupDF.write.mode("overwrite"), writeConfig)



val df = MongoSpark.load(sparkSession)
df.printSchema()




spark-submit --master yarn --class app.Main ./mot-mongo-assembly-0.1.0-SNAPSHOT.jar -test_result_dir=/user/maria_dev/mot_data/test_result -test_item_dir=/user/maria_dev/mot_data/test_item -lookup_data_dir=/user/maria_dev/mot_data/lookup -mongoserver=192.168.8.101




