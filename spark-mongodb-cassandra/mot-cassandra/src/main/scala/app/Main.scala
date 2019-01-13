package app


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
			println("Usage: -test_result_dir=[hdfspath] -test_item_dir=[hdfspath] -lookup_data_dir=[hdfspath] -cassandraHost=[ip]")
			System.exit(-1)
		}

		val testResultDir= params("test_result_dir")
		val testItemDir= params("test_item_dir")
		val lookupDir= params("lookup_data_dir")
		val cassandraHost = params("cassandraHost")

		//initialize spark
		val sparkSession = SparkSession.
			builder().
			appName("MotVehicleTest import (Cassandra)".
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