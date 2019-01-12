package app

import com.mongodb.spark._
import com.mongodb.spark.config._

import java.nio.file._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.bson._

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray


object Main {

	val TEST__RESULT__SCHEMA = "testresult.json"
	val TEST__ITEM__SCHEMA = "testitem.json"
	val TEST__DETAILS__SCHEMA = "testdetail.json"

	/**
	 * The purpose of this application is to read a file from hdfs (path will be passed in), 
	 * it will also perform some join with supplementary data and populate a mongdb database
	 * in append mode.
	 */
	def main(args: Array[String]) : Unit = {
		val params = parseArg(args)
		if (params.size < 4){
			println("Usage: -test_result_dir=[hdfspath] -test_item_dir=[hdfspath] -lookup_data_dir=[hdfspath] -schema_file_dir=[localpath]  -mongoserver=[ip]")
			System.exit(-1)
		}

		val testResultDir= params("test_result_dir")
		val testItemDir= params("test_item_dir")
		val lookupDir= params("supp_data_path")
		val schemaDir = params("schema_file_dir")
		val mongoHost = params("mongoserver")

		//initialize spark
		val sparkSession = SparkSession.
			builder().
			appName("MotVehicleTest import").
			enableHiveSupport().
			getOrCreate()

		import sparkSession.implicits._

		//create the dataframes
		val testItemDF = sparkSession.read.schema(readSchemaFromFile(schemaDir, TEST__ITEM__SCHEMA)).csv(testItemDir)
		val testResultDF = sparkSession.read.schema(readSchemaFromFile(schemaDir, TEST__RESULT__SCHEMA)).csv(testResultDir).cache
		val testDetailsDF = sparkSession.read.schema(readSchemaFromFile(schemaDir, TEST__DETAILS__SCHEMA)).csv(lookupDir)

		val enrichedResultDF = enrichResultDF(sparkSession, testResultDF, lookupDir)
		//write this into mongodb collection
		val writeConfig = WriteConfig("mot", "testresult", s"mongodb://$mongoHost/mot", 10000, WriteConcern.MAJORITY)
		MongoSpark.save(enrichedResultDF.write.mode("append"), writeConfig)

		val enrichedItemDF = enrichItemDF(sparkSession, testItemDF, testResultDF, testDetailsDF, lookupDir)
		//write this into mongodb collection
		val writeConfig = WriteConfig("mot", "testitem", s"mongodb://$mongoHost/mot", 10000, WriteConcern.MAJORITY)
		MongoSpark.save(enrichedItemDF.write.mode("append"), writeConfig)
	}


	def enrichResultDF(spark: SparkSession, baseDF: DataFrame, lookupDir: String) : DataFrame = {
		import spark.implicits._
		//get all the lookup tables
		val testTypeDF = readLookupTable(spark, lookupDir, "mdr_test_types")
		val testOutcomeDF = readLookupTable(spark, lookupDir, "mdr_test_outcome")
		val fuelTypeDF = readLookupTable(spark, lookupDir, "mdr_fuel_types")
		val itemLocationDF = readLookupTable(spark, lookupDir, "mdr_rfr_location")

		baseDF.join(testTypeDF, $"test_type" === $"type_code", "left_outer").
			join(testOutcomeDF, $"test_result" === $"result_code", "left_outer").
			join(fuelTypeDF, $"fuel_type" === fuelTypeDF("type_code"), "left_outer").
			select($"test_id",$"vehicle_id", $"test_date", $"test_class_id", 
				struct($"test_type".as("type_code"), testTypeDF("test_type").as("description")).as("test_type"), 
				struct($"result_code", testOutcomeDF("result").as("description")).as("test_result"), 
				struct(fuelTypeDF("type_code"), fuelTypeDF("fuel_type").as("description")).as("fuel_type"), 
				$"test_milage",$"postcode_area", $"make", $"model", 
				$"cylinder_capacity", $"first_use_date")
	}

	def enrichItemDF(spark: SparkSession, baseDF: DataFrame, testResultDF: DataFrame, testDetailsDF: DataFrame, lookupDir: String) : DataFrame = {
		val itemGroupDF = readLookupTable(spark, lookupDir, "item_group")
		val itemLocationDF = readLookupTable(spark, lookupDir, "mdr_rfr_location")

		testResultDF.join(baseDF, testResultDF("test_id") === baseDF("test_id")).
			join(testDetailsDF,  (testDetailsDF("rfr_id") === baseDF("rfr_id")) && 
				(testDetailsDF("test_class_id") === testResultDF("test_class_id"))).
			join(itemGroupDF, (itemGroupDF("test_item_id") === testDetailsDF("test_item_set_section_id")) && 
				itemGroupDF("test_class_id") === testDetailsDF("test_class_id")).
			join(itemLocationDF, (itemLocationDF("id") === baseDF("location_id"))).
			select(baseDF("test_id"), baseDF("rfr_id"), baseDF("rfr_type"), 
				baseDF("d_mark"), testDetailsDF("test_class_id"), 
				testDetailsDF("test_item_id"), testDetailsDF("minor_item"), testDetailsDF("rfr_desc"), 
				testDetailsDF("rfr_loc_marker"), testDetailsDF("rfr_insp_manual_desc"), 
				testDetailsDF("rfr_advisory_text"), testDetailsDF("test_item_set_section_id"), 
				itemGroupDF("parent_id"), itemGroupDF("test_item_set_section_id"), itemGroupDF("item_name"), 
				struct(baseDF("location_id"), itemLocationDF("lateral"), 
					itemLocationDF("longitudinal"), itemLocationDF("vertical")).as("location"))
	}

	
	private def readLookupTable(spark: SparkSession, lookupDir: String, tablename: String) : DataFrame = {
		spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).
			csv(s"lookupDir/$tablename")
	}
	
	private def readSchemaFromFile(schemaDir: String, schemaFile: String) : StructType = {
		import scala.io.Source

		val fileName = Paths.get(schemaDir, schemaFile).toFile.getPath
		val json = Source.fromFile(fileName).getLines.next
		DataType.fromJson(json).asInstanceOf[StructType]
	}


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
		valuesAndTypes.foreach((t: (Any, StructField)) => doc.put(t._2.name, conform(t._2, t._1)))
		doc
	}

	private def parseArg(args: Array[String]) : Map[String, String] = {
		args.map((arg: String) => {
			val parts = arg.split("=")

			(parts(0).toLowerCase().substring(1) -> parts(1))
		}).toMap
	}
}