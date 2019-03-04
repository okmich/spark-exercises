---
drop table test_volumn_by_class_and_outcome;
create table test_volumn_by_class_and_outcome (
	test_type_code text,
	test_result_code text,
	test_date date,
	test_class_id text,
	vehicle_id int,
	test_milage int,
	postcode_area varchar,
	make varchar,
	model varchar,
	cylinder_capacity int,
	first_use_date date,
	test_type varchar,
	test_result varchar,
	fuel_type_code varchar,
	fuel_type varchar,
	PRIMARY KEY ((test_type_code, test_result_code), test_date, test_class_id, test_result)
);



SELECT test_class_id,
	test_result,
	COUNT(*) AS TEST_VOLUME
FROM test_volumn_by_class_and_outcome
WHERE test_type_code='NT'
	AND test_result_code IN ('P','F','PRS')
	AND test_date >= '2009-04-01'
	AND test_date < '2010-03-31'
GROUP BY test_class_id, test_result;

