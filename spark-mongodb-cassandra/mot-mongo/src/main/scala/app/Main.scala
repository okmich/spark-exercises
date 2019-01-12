package app

import com.mongodb.WriteConcern
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
			println("Usage: -test_result_dir=[hdfspath] -test_item_dir=[hdfspath] -lookup_data_dir=[hdfspath] -mongoserver=[ip]")
			System.exit(-1)
		}

		val testResultDir= params("test_result_dir")
		val testItemDir= params("test_item_dir")
		val lookupDir= params("lookup_data_dir")
		val mongoHost = params("mongoserver")

		//initialize spark
		val sparkSession = SparkSession.
			builder().
			appName("MotVehicleTest import").
			enableHiveSupport().
			getOrCreate()

		import sparkSession.implicits._

		//create the dataframes
		val testItemDF = sparkSession.read.option("header", "true").option("sep", "|").
				schema(readSchemaFromFile(TEST__ITEM__SCHEMA)).csv(testItemDir)
		val testResultDF = sparkSession.read.option("header", "true").option("sep", "|").
				schema(readSchemaFromFile(TEST__RESULT__SCHEMA)).csv(testResultDir).cache
		val testDetailsDF = sparkSession.read.option("header", "true").option("sep", "|").
				schema(readSchemaFromFile(TEST__DETAILS__SCHEMA)).csv(lookupDir + "/item_details")

		//get all the lookup tables
		val itemGroupDF = readLookupTable(sparkSession, lookupDir, "item_group")
		val testTypeDF = readLookupTable(sparkSession, lookupDir, "mdr_test_types")
		val testOutcomeDF = readLookupTable(sparkSession, lookupDir, "mdr_test_outcome")
		val fuelTypeDF = readLookupTable(sparkSession, lookupDir, "mdr_fuel_types")
		val itemLocationDF = readLookupTable(sparkSession, lookupDir, "mdr_rfr_location")

		// MOT Test datasetmodel in mongo
		// ==============================
		// test_result
		// 	 test_type
		// 	 test_outcome
		// 	 fuel_type

		// 	 item_items
		// 		item_location
		// 		item_details
		// 		item_group

		//start building the mongodb full document 
		val parentIGDF = itemGroupDF.as("parentIGDF")
		val itemGroupJoinDF = itemGroupDF.join(parentIGDF, (itemGroupDF("parent_id") === $"parentIGDF.test_item_id") 
					&& (itemGroupDF("test_item_set_section_id") === $"parentIGDF.test_class_id"), "left_outer").
				select(itemGroupDF("test_item_id"), itemGroupDF("test_class_id"), 
					itemGroupDF("test_item_set_section_id"), itemGroupDF("item_name"), 
					struct($"parentIGDF.test_item_id", $"parentIGDF.item_name").as("parent"))

		val tdDF = testDetailsDF.join(itemGroupJoinDF, testDetailsDF("test_item_set_section_id") === itemGroupJoinDF("test_item_id") && 
			testDetailsDF("test_class_id") === itemGroupJoinDF("test_class_id"), "left_outer").
				select($"rfr_id", testDetailsDF("test_class_id"), testDetailsDF("test_item_id"), $"minor_item", $"rfr_desc", 
					$"rfr_loc_marker", $"rfr_insp_manual_desc", $"rfr_advisory_text", testDetailsDF("test_item_set_section_id"), 
					$"item_name".as("item_group_name"), $"parent")

		val enrichedTestItemDF = testItemDF.join(itemLocationDF, testItemDF("location_id") === itemLocationDF("id")).
			join(testResultDF, testItemDF("test_id") === testResultDF("test_id")).
			join(tdDF, testItemDF("rfr_id") === tdDF("rfr_id") && testResultDF("test_class_id") === tdDF("test_class_id"), "left_outer").
			select(testItemDF("test_id"), testItemDF("rfr_id"), $"rfr_type_code", 
				struct($"location_id", $"lateral", $"longitudinal", $"vertical").as("location"), 
				$"dangerous_mark", 
				struct(tdDF("test_class_id"), $"test_item_id", $"minor_item", $"rfr_desc", $"rfr_loc_marker", 
					$"rfr_insp_manual_desc", $"rfr_advisory_text", $"test_item_set_section_id", $"item_group_name", 
					$"rfr_advisory_text", $"parent").as("test_detail"))

		val groupedItemDF = enrichedTestItemDF.groupBy("test_id").agg(collect_set(
				struct($"test_id", $"rfr_id",  $"rfr_type_code", $"location", $"dangerous_mark", $"test_detail")
			).as("test_items"))

		val enrichedTestResultDF = testResultDF.join(fuelTypeDF, testResultDF("fuel_type") === fuelTypeDF("type_code"), "left_outer").
			join(testTypeDF, testResultDF("test_type") === testTypeDF("type_code"), "left_outer").
			join(testOutcomeDF, testResultDF("test_result") === testOutcomeDF("result_code"), "left_outer").
			join(groupedItemDF, testResultDF("test_id") === groupedItemDF("test_id"), "left_outer").
				select(testResultDF("test_id"), $"vehicle_id", $"test_date", $"test_class_id", 
				struct(testResultDF("test_type").as("type_code"), testTypeDF("test_type")).as("test_type"),
				struct(testResultDF("test_result").as("result_code"), testOutcomeDF("result")).as("outcome"),
				$"test_mileage", $"postcode_area", $"make", $"model", $"colour", 
				struct(testResultDF("fuel_type").as("fuel_type_code"), fuelTypeDF("fuel_type")).as("fuel"),
				$"cylinder_capacity", $"first_use_date", $"test_items")	

		//write this into mongodb collection
		val writeConfig = WriteConfig("mot", "test_result", s"mongodb://$mongoHost/mot", 10000, WriteConcern.W1)
		MongoSpark.save(enrichedTestResultDF.write.mode("append"), writeConfig)
	}
	
	private def readLookupTable(spark: SparkSession, lookupDir: String, tablename: String) : DataFrame = {
		spark.read.options(Map("sep" -> "|", "header" -> "true", "inferSchema" -> "true")).
			csv(s"$lookupDir/$tablename")
	}
	
	private def readSchemaFromFile(schemaFile: String) : StructType = {
		import scala.io.Source

		val fileStream = getClass.getClassLoader.getResourceAsStream(s"schema/$schemaFile")
		val json = Source.fromInputStream(fileStream).getLines.next
		DataType.fromJson(json).asInstanceOf[StructType]
	}

	private def parseArg(args: Array[String]) : Map[String, String] = {
		args.map((arg: String) => {
			val parts = arg.split("=")

			(parts(0).toLowerCase().substring(1) -> parts(1))
		}).toMap
	}
}