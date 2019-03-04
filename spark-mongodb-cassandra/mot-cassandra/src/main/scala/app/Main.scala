package app

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

import java.nio.file._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
		if (params.size < 5){
			println("Usage: -test_result_dir=[hdfspath] -test_item_dir=[hdfspath] -lookup_data_dir=[hdfspath] -cassandra_host=[ip] -model=[model_to_load]")
			System.exit(-1)
		}

		val testResultDir= params("test_result_dir")
		val testItemDir= params("test_item_dir")
		val lookupDir= params("lookup_data_dir")
		val cassandraHost = params("cassandra_host")
		val loadModel = params("model")

		//initialize spark
		val sparkSession = SparkSession.
			builder().
			appName("MotVehicleTest import (Cassandra)").
			config("spark.cassandra.connection.host", cassandraHost).
			getOrCreate()

		import sparkSession.implicits._

		//create the dataframes
		val testItemDF = sparkSession.read.option("header", "true").option("sep", "|").
				schema(readSchemaFromFile(TEST__ITEM__SCHEMA)).csv(testItemDir)
		val testDetailsDF = sparkSession.read.option("header", "true").option("sep", "|").
				schema(readSchemaFromFile(TEST__DETAILS__SCHEMA)).csv(lookupDir + "/item_details")

		//get all the lookup tables
		val itemGroupDF = readLookupTable(sparkSession, lookupDir, "item_group")
		val testTypeDF = readLookupTable(sparkSession, lookupDir, "mdr_test_types")
		val testOutcomeDF = readLookupTable(sparkSession, lookupDir, "mdr_test_outcome")
		val fuelTypeDF = readLookupTable(sparkSession, lookupDir, "mdr_fuel_types")
		val itemLocationDF = readLookupTable(sparkSession, lookupDir, "mdr_rfr_location")

		loadModel.toLowerCase match {
			case "class" => {
				val testDetailModel = getTestDetalsWithItemGroupHierarchy(
						sparkSession, testDetailsDF, itemGroupDF)
				
				//save to data store
				testDetailModel.write.
				  format("org.apache.spark.sql.cassandra").
				  options(Map("table" -> "test_item_details", "keyspace" -> "motks")).
				  option("spark.cassandra.input.consistency.level", "ANY").
				  save()
			}
			case "results" => {
				val testResultDF = sparkSession.read.option("header", "true").option("sep", "|").
						schema(readSchemaFromFile(TEST__RESULT__SCHEMA)).csv(testResultDir).cache
				val enrichedTestResultDF = generateTestResultModel(sparkSession,
							testResultDF, fuelTypeDF, testTypeDF, testOutcomeDF)

				//save to data store
				enrichedTestResultDF.write.
				  format("org.apache.spark.sql.cassandra").
				  options(Map("table" -> "test_results", "keyspace" -> "motks")).
				  save()
			}
			case "items" => {
				//read the test_results from the database
				val testResultModel = sparkSession.read.
						format("org.apache.spark.sql.cassandra").
						options(Map( "table" -> "test_result", "keyspace" -> "motks" )).
  						load()

				val enrichedTestItemDF = testItemDF.join(itemLocationDF, testItemDF("location_id") === itemLocationDF("id")).
					join(testResultModel, testItemDF("test_id") === testResultModel("test_id")).
					select(
						testItemDF("test_id"), testResultModel("vehicle_id"),
						testResultModel("test_date"), testResultModel("test_type_code"), 
						testResultModel("test_result_code"), testResultModel("test_class_id"),
						$"rfr_type_code".as("rfr_type"),
						$"test_mileage", $"postcode_area", $"make", $"model", $"cylinder_capacity", 
						$"first_use_date", $"test_type", $"test_result", $"fuel_type_code", $"fuel_type", 

						testItemDF("rfr_id"), $"location_id", $"lateral", $"longitudinal", $"vertical",
						$"dangerous_mark", 

						testResultModel("test_item_id"), $"minor_item", $"rfr_desc", $"rfr_loc_marker",
						$"rfr_insp_manual_desc", $"rfr_advisory_text",
						$"test_item_set_section_id", $"parent_level_1", $"parent_level_2", 
						$"parent_level_3", $"parent_level_4", $"parent_level_5")

				//save to data store
				enrichedTestItemDF.write.
				  format("org.apache.spark.sql.cassandra").
				  options(Map("table" -> "test_items", "keyspace" -> "motks")).
				  save()
			}
			case _ => throw new IllegalArgumentException("Unknown model - try 'class/results/items'")
		}
	}

	private def getTestDetalsWithItemGroupHierarchy(spark: SparkSession, 
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
				select(baseDF("test_class_id"), 
					$"rfr_id",
					baseDF("test_item_id"), 
					$"minor_item", 
					$"rfr_desc", 
					$"rfr_loc_marker", 
					$"rfr_insp_manual_desc", 
					$"rfr_advisory_text", 
					detailsDF("test_item_set_section_id"),
					baseDF("item_name").as("item_group_level_1"), 
					$"level1DF.item_name".as("item_group_level_2"),
					$"level2DF.item_name".as("item_group_level_3"),
					$"level3DF.item_name".as("item_group_level_4"),
					$"level4DF.item_name".as("item_group_level_5"))
	}

	private def generateTestResultModel(spark: SparkSession, 
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