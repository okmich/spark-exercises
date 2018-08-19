// window analytical function
import org.apache.spark.sql.expressions.Window
val crimeYearDistrictCountDF = crimeDS.groupBy("crime_type", "district", "year").count

val windowPartition = Window.partitionBy("year", "district").orderBy(desc("count"))
val rankingDF = crimeYearDistrictCountDF.withColumn("crime_rank", rank().over(windowPartition)).filter($"crime_rank" <= 10)

rankingDF.filter("year=2002").show
rankingDF.filter("year=2002 and district=11").show
rankingDF.filter("year=2002 and district=2").show

// time window analytical function
//convert the reported date field to a timestamp crime_type
val newDF = crimeDS.
	select("crime_type", "district", "year", "date").
	withColumn("incident_dt", to_timestamp($"date", "MM/dd/yyyy hh:mm:ss a")).
	drop("date")

//run the time fixed-sized window function over this field
val tWindowDF = newDF.withColumn("window_dt", window($"incident_dt", "1 week"))
// run a weekly count of theft for 2002
val aggDF = tWindowDF.groupBy("window_dt", "year", "district", "crime_type").count
aggDF.write.saveAsTable("crime.crime_window_count")

// run a time sliding widow function
val sWindowDF = newDF.withColumn("window_dt", window($"incident_dt", "1 week", "1 day"))
