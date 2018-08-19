// window analytical function
import org.apache.spark.sql.expressions.Window
val crimeDistrictCountDF = crimeDS.groupBy("crime_type", "district").count
val windowPartition = Window.partitionBy("district").orderBy(desc("count"))
val rankingDF = crimeDistrictCountDF.withColumn("distict_rank", rank().over(windowPartition))


// time window analytical function
//convert the reported date field to a timestamp crime_type
//run the window function over this field



//

