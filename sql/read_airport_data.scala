

val airportDF = spark.read.options(Map("header" -> "true", "sep"->",", "inferSchema" -> "true")).
csv("/user/maria_dev/rawdata/flight_data/airports").cache

val airport2DF = airportDF.select(
$"iata".as("iata2"), $"airport".as("airport2"), $"city".as("city2"), 
$"state".as("state2"), $"country".as("country2"), $"lat".as("lat2"), 
$"long".as("long2"))

val pairDF = airportDF.crossJoin(airport2DF).
filter($"iata" =!= $"iata2")

spark.sql("select * from flight_performance.flight f join flight_performance.airport a on f.origin = a.iata").show

val s = """
	select * from 
	(select * from flight_performance.flight cluster by origin) f 
	join (select * from flight_performance.airport cluster by iata) a on f.origin = a.iata
"""

spark.sql(s).show