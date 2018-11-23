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
	(int)(actualelapsedtime == 'na' ? null : actualelapsedtime) as actualelapsedtime,
	(int)(crselapsedtime == 'na' ? '' : crselapsedtime) as crselapsedtime,
	(int)(airtime == 'na' ? '' : airtime) as airtime,
	(int)(arrdelay == 'na' ? '' : arrdelay) as arrdelay,
	(int)(depdelay == 'na' ? '' : depdelay) as depdelay,
	(origin == 'na' ? '' : origin) as origin,
	(dest == 'na' ? '' : dest) as dest,
	(int)(distance == 'na' ? '' : distance) as distance,
	(int)(taxiin == 'na' ? '' : taxiin) as taxiin,
	(int)(taxiout == 'na' ? '' : taxiout) as taxiout,
	(cancelled == 'na' ? '' : cancelled) as cancelled,
	(cancellationcode == 'na' ? '' : cancellationcode) as cancellationcode,
	(diverted == 'na' ? '' : diverted) as diverted,
	(int)(carrierdelay == 'na' ? '' : carrierdelay) as carrierdelay, 
	(int)(weatherdelay == 'na' ? '' : weatherdelay) as weatherdelay, 
	(int)(nasdelay == 'na' ? '' : nasdelay) as nasdelay, 
	(int)(securitydelay == 'na' ? '' : securitydelay) as securitydelay, 
	(int)(lateaircraftdelay == 'na' ? '' : lateaircraftdelay) as lateaircraftdelay;

STORE rel_data INTO '/user/maria_dev/processed/flight_data/flights' USING OrcStorage('-c ZLIB');