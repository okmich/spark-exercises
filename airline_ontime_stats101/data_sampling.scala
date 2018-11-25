//data sampling
val flightDF = spark.read.table("airline_performance.flight").
select("year", "month", "dayofmonth", "dayofweek", "dest", "origin", "uniquecarrier", "depdelay", "arrdelay", "depdelay").cache

//sample using data frame
flightDF.sample(false, 0.1)

//sample using data frame using a seed
flightDF.sample(false, 0.1, 3440303)

//==================================
//stratified sampling
//==================================
flightDF.groupBy("year").count.show

//sample by year - 50%
val stratSampleDF = flightDF.stat.sampleBy("year", Map("2006" -> 0.5,"2007" -> 0.5), 1234567)

//sample 20% of each year's value by month such that we take 20% of each month
val stratSampleDF2006 = stratSampleDF.where($"year" === 2006)
val stratSampleDF2007 = stratSampleDF.where($"year" === 2007)

def sampleByField(df: DataFrame, f: String, percent: Double) : DataFrame = {
	val fractions = df.groupBy(f).count.collect.
		map((r:Row) => r.getAs[Int](0) -> percent).toMap

	df.stat.sampleBy(f, res22, 1234567)
}

//create a sample of proportional number of years and month
val sampleDF = sampleByField(stratSampleDF2006, "month", 0.2) union sampleByField(stratSampleDF2007, "month", 0.2)

//use the relative frequence function to verify that the 
//frequencies in the sample and the population is approximate or close enough
//assignment
