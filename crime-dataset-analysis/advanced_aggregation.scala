// what is a grouping set 
// not in spark dataset 

// rollup

// for each ward and crime type, return the number of recorded crime with rollup
// in sql : select ward, crime_type, count(1) from crime group by ward, crime_type with rollup
crimeDS.rollup("year", "crime_type").count.orderBy("year").show(1300)


// cube
crimeDS.cube("year", "crime_type").count.orderBy("year").show(1300)

// pivot
// for each year, give the 10 top crime_types in a pivot 
// this 4 lines are from the windows_aggregation.scala note
import org.apache.spark.sql.expressions.Window
val crimeYearDistrictCountDF = crimeDS.groupBy("crime_type", "year").count
val windowPartition = Window.partitionBy("year").orderBy(desc("count"))
val rankingDF = crimeYearDistrictCountDF.withColumn("crime_rank", rank().over(windowPartition)).filter($"crime_rank" <= 10)

val pivotDF = rankingDF.select("year", "crime_type", "crime_rank").groupBy("crime_type").pivot("year").max("crime_rank")

//can you do the same for district X for all years and all crime_types


///FROM HIVE PERSPECTIVE 
// select ward, crime_type, count(1) from crime group by ward, crime_type with rollup
// is the same as 
// select ward, crime_type, count(1) from crime group by ward, crime_type GROUPING SETS ((ward, crime_type), (ward), ())
// which is also the same as 
// select ward, crime_type, count(1) from crime group by ward, crime_type 
// union all
// select ward, null, count(1) from crime group by ward
// union all	
// select null, null, count(1) from crime 


// select ward, crime_type, count(1) from crime group by ward, crime_type with cube
// is the same as 
// select ward, crime_type, count(1) from crime group by ward, crime_type GROUPING SETS ((ward, crime_type), (ward), (crime_type), ())
// is the same as 
// select ward, crime_type, count(1) from crime group by ward, crime_type 
// union all
// select ward, null, count(1) from crime group by ward
// union all
// select null, crime_type, count(1) from crime group by crime_type
// union all
// select null, null, count(1) from crime 