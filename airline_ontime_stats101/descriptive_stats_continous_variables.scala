//descriptive statistics for continuous variables

val flightDF = spark.read.table("airline_performance.flight").
select("year", "month", "dayofmonth", "dayofweek", "depdelay", "arrdelay", "depdelay", "carrierdelay", "weatherdelay", "nasdelay", "securitydelay", "lateaircraftdelay").cache

//describe continuous variables using describe
flightDF.select("arrdelay", "depdelay", "carrierdelay", "weatherdelay", "nasdelay", "securitydelay", "lateaircraftdelay").describe().show

//describe continuous variables using summary
flightDF.select("arrdelay", "depdelay", "carrierdelay", "weatherdelay", "nasdelay", "securitydelay", "lateaircraftdelay").summary().show

//get the various values or percentiles for a single field
flightDF.stat.approxQuantile("arrdelay", Array(0.1,0.15,0.85,0.9), 0.005)

//get the various values or percentiles for more than one field
flightDF.stat.approxQuantile(Array("arrdelay", "depdelay"), Array(0.1, 0.15, 0.85, 0.9), 0.005)

//covariance
flightDF.stat.cov("arrdelay", "depdelay")

//correlation
flightDF.stat.corr("arrdelay", "depdelay")




