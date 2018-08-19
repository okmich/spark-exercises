// for each ward and crime type, return the number of recorded crime
// in sql : select ward, crime_type, count(1) from crime group by ward, crime_type
//
// crimeDS.createOrReplaceTempView("crime")
// def run(s:String): Unit = spark.sql(s).show(1000)
// run("select ward, crime_type, count(1) from crime group by ward, crime_type")

val recordCrimeByWardAndType = crimeDS.groupBy("ward", "crime_type").count
recordCrimeByWardAndType.show

// for each year and district, get the percentage of crime_type that led to arrest
val df = crimeDS.groupBy("year", "district").count
val df1 = crimeDS.filter(_.ledToArrest).groupBy("year", "district").count

val jdf = df1.join(df, df("year") === df1("year") && df("district") === df1("district")).
	select(df("year"), df("district"), df1("count")/df("count"))

// get the type of domestic crime that gets the most arrest. 

// try and see if this is consistent for all years


