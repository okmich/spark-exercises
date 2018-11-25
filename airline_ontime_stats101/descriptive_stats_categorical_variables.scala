//descriptive statistics for categorical variables

val airports = spark.read.table("airline_performance.airports").where("iata <> 'iata'")
//to get the total number of airports
airports.count

//to see fields layout
airports.printSchema

val airportDF = airports.select("iata", "airport", "city", "state", "country").cache

//to describe categorical variables
airportDF.describe().show



// analyzing the plane records
val planeInfo = spark.read.table("airline_performance.plane_info").where("tailnum <> 'tailnum'").cache

//get the dataframe stat function object
val planeInfoDFStats = planeInfo.stat

// 
import org.apache.spark.sql._
def frequencyDist(df: DataFrame, col: String, orderBy: String = null) : DataFrame = {
	val freqDF = df.groupBy(col).count
	if (orderBy == null) freqDF
	else if (orderBy.equalsIgnoreCase("desc")) freqDF.orderBy($"count".desc)
	else freqDF.orderBy($"count")
}

import org.apache.spark.sql._
def relFrequencyDist(df: DataFrame, col: String, orderBy: String = null) : DataFrame = {
	val total = df.count
	val freqDF = df.groupBy(col).agg(((count(col)/total)).as("proportion"))
	if (orderBy == null) freqDF
	else if (orderBy.equalsIgnoreCase("desc")) freqDF.orderBy($"proportion".desc)
	else freqDF.orderBy($"proportion")
}


//