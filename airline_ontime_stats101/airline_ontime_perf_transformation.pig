raw_data = LOAD '/user/maria_dev/rawdata/flight_data/flights' USING PigStorage(',') as 
	(year:chararray, month:chararray, dayofmonth:chararray, dayofweek:chararray, deptime:chararray,
	crsdeptime:chararray, arrtime:chararray, crsarrtime:chararray, uniquecarrier:chararray, flightnum:chararray,
	tailnum:chararray, actualelapsedtime:chararray, crselapsedtime:chararray, airtime:chararray, arrdelay:chararray,
	depdelay:chararray,origin:chararray,dest:chararray,distance:chararray,taxiin:chararray, taxiout:chararray,
	cancelled:chararray,cancellationcode:chararray,diverted:chararray,carrierdelay:chararray,weatherdelay:chararray,
	nasdelay:chararray,securitydelay:chararray,lateaircraftdelay:chararray);

headless_data = FILTER raw_data BY (year != 'year');
rel_data = FOREACH headless_data GENERATE year, month, dayofmonth, dayofweek, 
	(deptime == 'na' ? '' : deptime) as deptime,
	(crsdeptime == 'na' ? '' : crsdeptime) as crsdeptime,
	(arrtime == 'na' ? '' : arrtime) as arrtime,
	(crsarrtime == 'na' ? '' : crsarrtime) as crsarrtime,
	(uniquecarrier == 'na' ? '' : uniquecarrier) as uniquecarrier,
	(flightnum == 'na' ? '' : flightnum) as flightnum,
	(tailnum == 'na' ? '' : tailnum) as tailnum,
	(actualelapsedtime == 'na' ? '' : actualelapsedtime) as actualelapsedtime,
	(crselapsedtime == 'na' ? '' : crselapsedtime) as crselapsedtime,
	(airtime == 'na' ? '' : airtime) as airtime,
	(arrdelay == 'na' ? '' : arrdelay) as arrdelay,
	(depdelay == 'na' ? '' : depdelay) as depdelay,
	(origin == 'na' ? '' : origin) as origin,
	(dest == 'na' ? '' : dest) as dest,
	(distance == 'na' ? '' : distance) as distance,
	(taxiin == 'na' ? '' : taxiin) as taxiin,
	(taxiout == 'na' ? '' : taxiout) as taxiout,
	(cancelled == 'na' ? '' : cancelled) as cancelled,
	(cancellationcode == 'na' ? '' : cancellationcode) as cancellationcode,
	(diverted == 'na' ? '' : diverted) as diverted,
	(carrierdelay == 'na' ? '' : carrierdelay) as carrierdelay, 
	(weatherdelay == 'na' ? '' : weatherdelay) as weatherdelay, 
	(nasdelay == 'na' ? '' : nasdelay) as nasdelay, 
	(securitydelay == 'na' ? '' : securitydelay) as securitydelay, 
	(lateaircraftdelay == 'na' ? '' : lateaircraftdelay) as lateaircraftdelay;

STORE rel_data INTO '/user/maria_dev/processed/flight_data/flights' USING OrcStorage('-c ZLIB');