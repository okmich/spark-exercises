// -- Initial, Completed Test Volumes by Class 2009-10 (As calculated in DVSA effectiveness report)
// -- ==============
db.test_result.aggregate([
    {$match : {$and : [{'outcome.result_code' : {$in: ['P', 'F', 'PRS' ]}}, {$and : [{'test_date' : {$gt : new ISODate('2004-04-01')}}, {'test_date' : {$lte : new ISODate('2005-03-31')}}]}]}}, 
    {$project:  {'test_class_id' : 1, 'test_result': '$outcome.result', '_id' : 0}},
    {$group : {
            _id : {'test_class_id' : '$test_class_id', 'test_result' : '$test_result'},
            'test_volume' : {$sum : 1}
        }
    },
    {$project : {'test_class_id' : '$_id.test_class_id', 'test_result' : '$_id.test_result', 'test_volume': 1, '_id':0}}
])


// -- RfR Volumes and Distinct Test Failures 2008 for Class 7 Vehicles by Top Level est Item Group (For vehicles as presented for initial test)
// -- ==================
db.test_result.aggregate([


])



// -- Basic Expansion of RfR Hierarchy for Class 5 Vehicles
// -- ==================
