CREATE SCHEMA mot;

USE mot;

CREATE TABLE testresult (
	testid INT UNSIGNED,
	vehicleid INT UNSIGNED,
	testdate DATE,
	testclassid CHAR(2),
	testtype CHAR(2),
	testresult CHAR(5),
	testmileage INT UNSIGNED,
	postcoderegion CHAR(2),
	make CHAR(50),
	model CHAR(50),
	colour CHAR(16),
	fueltype CHAR(2),
	cylcpcty INT UNSIGNED,
	firstusedate DATE,
	PRIMARY KEY (testid),
	INDEX IDX1 (testdate, testtype, testresult, testclassid) 
);


CREATE TABLE testitem (
	testid INT UNSIGNED,
	rfrid SMALLINT UNSIGNED,
	rfrtype CHAR(1),
	locationid INT,
	dmark CHAR(1),
	INDEX IDX1 (testid),
	INDEX IDX2 (rfrid)
);

CREATE TABLE testitem_detail (
	rfrid SMALLINT UNSIGNED,
	testclassid CHAR(2),
	tstitmid SMALLINT UNSIGNED,
	minoritem CHAR(1),
	rfrdesc VARCHAR(250),
	rfrlocmarker CHAR(1),
	rfrinspmandesc VARCHAR(500),
	rfradvisorytext VARCHAR(250),
	tstitmsetsecid SMALLINT UNSIGNED,
	PRIMARY KEY (rfrid, testclassid),
	INDEX IDX1 (tstitmid, testclassid),
	INDEX IDX2 (tstitmsetsecid, testclassid)
);

CREATE TABLE testitem_group (
	tstitmid SMALLINT UNSIGNED,
	testclassid CHAR(2),
	parentid SMALLINT UNSIGNED,
	tstitmsetsecid SMALLINT UNSIGNED,
	itemname CHAR(100),
	PRIMARY KEY (tstitmid, testclassid),
	INDEX IDX1 (parentid, testclassid),
	INDEX IDX2(tstitmsetsecid, testclassid)
);


CREATE TABLE failure_location (
	failurelocationid INT(4),
	lateral CHAR(20),
	vertical CHAR(20),
	longitudinal CHAR(20),
	PRIMARY KEY (failurelocationid)
);

CREATE TABLE fuel_type (
	type_code CHAR(4),
	fuel_type CHAR(40),
	PRIMARY KEY (type_code)
);

CREATE TABLE test_type (
	type_code CHAR(4),
	test_type CHAR(40),
	PRIMARY KEY (type_code)
);

CREATE TABLE test_outcome (
	result_code CHAR(6),
	result CHAR(40),
	PRIMARY KEY (result_code)
);

-- write to testitem_detail
LOAD DATA LOCAL INFILE 'D:\\data_dump\\MOT-test-datasets\\item_detail.txt'
INTO TABLE testitem_detail
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- write to testitem_group
LOAD DATA LOCAL INFILE 'D:\\data_dump\\MOT-test-datasets\\item_group.txt'
INTO TABLE testitem_group
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- write to failure location
LOAD DATA LOCAL INFILE 'D:\\data_dump\\MOT-test-datasets\\mdr_rfr_location.txt'
INTO TABLE failure_location
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- write to fuel_type
LOAD DATA LOCAL INFILE 'D:\\data_dump\\MOT-test-datasets\\mdr_fuel_types.txt'
INTO TABLE fuel_type
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- write to test_type
LOAD DATA LOCAL INFILE 'D:\\data_dump\\MOT-test-datasets\\mdr_test_types.txt'
INTO TABLE test_type
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- write to test_outcome
LOAD DATA LOCAL INFILE 'D:\\data_dump\\MOT-test-datasets\\mdr_test_outcome.txt'
INTO TABLE test_outcome
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- write to test_item for 2005
LOAD DATA LOCAL INFILE 'D:\\data_dump\\MOT-test-datasets\\test_item_2010.txt'
INTO TABLE testitem
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- write to testresult for 2005
LOAD DATA LOCAL INFILE 'D:\\data_dump\\MOT-test-datasets\\test_result_2010.txt'
INTO TABLE testresult
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
