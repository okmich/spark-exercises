-- Test Result - This contains details of individual MOT tests and of the vehicle tested. All tests which could result in a valid pass result are included. Datasets are provided by calendar year and can be concatenated if required.

-- Test Item - This contains details of individual MOT test failure items and advisory notices. Datasets are split by calendar year and can be concatenated if required.

-- Test Item Detail - This contains details of individual RfRs

-- Test Item Group - This contains details of RfR groupings within the test item hierarchy. The top level group for a Test Class is always ‘Vehicle’, with a Test Item ID of 0. 

-- Failure Location - Reference for Location IDs in Test Item Table


-- Initial, Completed Test Volumes by Class 2009-10 (As calculated in DVSA effectiveness report)
-- ==============
SELECT TESTCLASSID
	,TESTRESULT
	,COUNT(*) AS TEST_VOLUME
FROM TESTRESULT
WHERE TESTTYPE=’NT’
	AND TESTRESULT IN (‘P’,’F’,’PRS’)
	AND TESTDATE BETWEEN ‘2009-04-01’ AND ‘2010-03-31’
GROUP BY TESTCLASSID
	,TESTRESULT;


-- RfR Volumes and Distinct Test Failures 2008 for Class 7 Vehicles by Top Level est Item Group (For vehicles as presented for initial test)
-- ==============
SELECT d.ITEMNAME
	,COUNT(*) AS RFR_VOLUME
	,COUNT(DISTINCT a.TESTID) AS TEST_VOLUME
FROM TESTRESULT AS a
	INNER JOIN TESTITEM AS b
		ON a.TESTID=b.TESTID
	INNER JOIN TESTITEM_DETAIL AS c
		ON b.RFRID=c.RFRID
		AND a.TESTCLASSID = c.TESTCLASSID
	INNER JOIN TESTITEM_GROUP AS d
		ON c.TSTITMSETSECID = d.TSTITMID
		AND c.TESTCLASSID = d.TESTCLASSID
WHERE a.TESTDATE BETWEEN ‘2008-01-01’ AND ‘2008-12-31’
	AND a.TESTCLASSID = ‘7’
	AND a.TESTTYPE=’NT’
	AND a.TESTRESULT IN (‘F’,’PRS’)
	AND b.RFRTYPE IN(‘F’,’P’)
GROUP BY d.ITEMNAME;

-- Basic Expansion of RfR Hierarchy for Class 5 Vehicles
-- ==================
SELECT a.RFRID
	,a.RFRDESC
	,b.ITEMNAME AS LEVEL1
	,c.ITEMNAME AS LEVEL2
	,d.ITEMNAME AS LEVEL3
	,e.ITEMNAME AS LEVEL4
	,f.ITEMNAME AS LEVEL5
FROM TESTITEM_DETAIL AS a
	INNER JOIN TESTITEM_GROUP AS b
		ON a.TSTITMID = b.TSTITMID
		AND a.TESTCLASSID = b.TESTCLASSID
	LEFT JOIN TESTITEM_GROUP AS c
		ON b.PARENTID = c.TSTITEMID
		AND b.TESTCLASSID = c.TESTCLASSID
	LEFT JOIN TESTITEM_GROUP AS d
		ON c.PARENTID = d.TSTITEMID
		AND c.TESTCLASSID = d.TESTCLASSID
	LEFT JOIN TESTITEM_GROUP AS e
		ON d.PARENTID = e.TSTITEMID
		AND d.TESTCLASSID = e.TESTCLASSID
	LEFT JOIN TESTITEM_GROUP AS f
		ON e.PARENTID = f.TSTITMID
		AND e.TESTCLASSID = f.TESTCLASSID
WHERE a.TESTCLASSID = ‘5’;
