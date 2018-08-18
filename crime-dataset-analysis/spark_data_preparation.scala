val rawDF = spark.read.
	options(Map("header" -> "true", "quote" -> "\"", "inferSchema" -> "true")).
	csv("/user/maria_dev/rawdata/chicago/crime/Crimes_-_2001_to_present.csv")

// never take inferSchema into production. It is an unnecessary time-killer

val crimeDF = rawDF.withColumnRenamed("Case Number", "case_no").
	withColumnRenamed("Location Description", "location_desc").
	withColumnRenamed("Primary Type", "crime_type").
	withColumnRenamed("X Coordinate", "x_coord").
	withColumnRenamed("Y Coordinate", "y_coord").
	withColumnRenamed("Domestic", "domestic").
	withColumnRenamed("Updated On", "update_dt").
	withColumnRenamed("District", "district").
	withColumnRenamed("Community Area", "community").
	withColumnRenamed("FBI Code", "fbi_code").
	select("id", "case_no", "date", "block", "crime_type", "description", "location_desc", 
		"arrest", "domestic", "district", "ward", "community", "update_dt", "year", 
		"latitude", "longitude")


crimeDF.printSchema