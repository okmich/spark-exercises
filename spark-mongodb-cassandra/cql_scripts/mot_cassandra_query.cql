USE motks;
-- Initial, Completed Test Volumes by Class 2009-10 (As calculated in DVSA effectiveness report)
-- ==============
SELECT test_class_id,
	test_result,
	COUNT(*) AS TEST_VOLUME
FROM test_result
WHERE test_type_code='NT'
	AND test_result_code IN ('P','F','PRS')
	AND test_date >= '2009-04-01'
	AND test_date < '2010-03-31'
GROUP BY test_class_id, test_result;


-- RfR Volumes and Distinct Test Failures 2008 for Class 7 Vehicles by Top Level est Item Group (For vehicles as presented for initial test)
-- ==============
SELECT item_group_levels,
	COUNT(*) AS rfr_volume,
	COUNT(DISTINCT test_id) AS test_volume
FROM test_item
WHERE test_date >= '2008-01-01' AND test_date < '2008-12-31'
	AND test_class_id = '7'
	AND test_type_code = 'NT'
	AND test_result_code IN ('F','PRS')
	AND rfr_type IN ('F','P')
GROUP BY item_group_levels;

-- Basic Expansion of RfR Hierarchy for Class 5 Vehicles
-- ==================
SELECT rfr_id, rfr_desc, 
	item_group_level_1 AS level1,
	item_group_level_2 AS level2,
	item_group_level_3 AS level3,
	item_group_level_4 AS level4,
	item_group_level_5 AS level5
FROM test_item_details
WHERE test_class_id = '5';

