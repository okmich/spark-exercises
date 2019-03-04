CREATE KEYSPACE motks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE motks;

drop table IF EXISTS test_result;
CREATE TABLE IF NOT EXISTS test_result (
	test_id int,
	vehicle_id int,
	test_date date,
	test_type_code varchar,
	test_result_code varchar,
	test_class_id varchar,
	test_mileage int,
	postcode_area varchar,
	make varchar,
	model varchar,
	cylinder_capacity int,
	first_use_date date,
	test_type varchar,
	test_result varchar,
	fuel_type_code varchar,
	fuel_type varchar,
	PRIMARY KEY (test_id, test_date, test_type_code, test_result_code, test_class_id, vehicle_id)
);

drop table IF EXISTS test_result;
CREATE TABLE IF NOT EXISTS test_item (
	test_id int,
	test_date date,
	test_type_code varchar,
	test_result_code varchar,
	test_class_id varchar,
	rfr_type varchar,
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
	rfr_id smallint,
	location_id int,
	lateral varchar,
	longitudinal varchar,
	vertical varchar,
	dangerous_mark varchar, 
	test_item_id int,
	minor_item text,
	rfr_desc text,
	rfr_loc_marker text,
	rfr_insp_manual_desc text,
	rfr_advisory_text text,
	test_item_set_section_id int,
	item_group_levels map<varchar, varchar>,
	PRIMARY KEY (test_id, test_date, test_type_code, test_result_code, test_class_id, rfr_type, vehicle_id)
);


CREATE TABLE IF NOT EXISTS test_item_details (
	test_class_id varchar,
	rfr_id varchar,
	test_item_id int,
	minor_item text,
	rfr_desc text,
	rfr_loc_marker text,
	rfr_insp_manual_desc text,
	rfr_advisory_text text,
	test_item_set_section_id int,
	item_group_level_1 varchar,,
	item_group_level_2 varchar,
	item_group_level_3 varchar,
	item_group_level_4 varchar,
	item_group_level_5 varchar,
	PRIMARY KEY(test_class_id, rfr_id)
);

