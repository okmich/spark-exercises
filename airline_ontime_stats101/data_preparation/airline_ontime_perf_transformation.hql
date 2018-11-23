create database airline_performance;

use airline_performance;

-- create external table for flight
create external table flight
( year string, month string, dayofmonth string, dayofweek string,
  deptime string, crsdeptime string, arrtime string, crsarrtime string, 
  uniquecarrier string, flightnum string, tailnum string, 
  actualelapsedtime int, crselapsedtime int, airtime int, 
  arrdelay int, depdelay int, 
  origin string, dest string, 
  distance int, taxiin int, taxiout int,
  cancelled string, cancellationcode string, diverted string, 
  carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, lateaircraftdelay int
 ) stored as orc
location '/user/maria_dev/processed/flight_data/flights';


-- create external table for airports
create external table airports (
    iata string, 
    airport string, 
    city string,
    state string, 
    country string, 
    geolat float, 
    geolong float
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
) 
location '/user/maria_dev/rawdata/flight_data/airports';


-- create external table for carriers
create external table carriers (
    code varchar(4), 
    description varchar(30)
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)  
stored as textfile
location '/user/maria_dev/rawdata/flight_data/airports';


-- create external table for plane information
create external table plane_info (
    tailnum varchar(4), 
    type varchar(30),
    manufacturer string,
    issue_date varchar(16), 
    model varchar(10), 
    status varchar(10),
    aircraft_type varchar(30),
    pyear int
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
) 
stored as textfile
location '/user/maria_dev/rawdata/flight_data/planes';
