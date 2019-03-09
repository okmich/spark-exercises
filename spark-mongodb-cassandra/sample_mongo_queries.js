// -- Initial, Completed Test Volumes by Class 2009-10 (As calculated in DVSA effectiveness report)
// -- ==============
db.test_result.aggregate([
    {$match : {$and : [{'outcome.result_code' : {$in: ['P', 'F', 'PRS' ]}}, {$and : [{'test_date' : {$gt : new ISODate('2004-04-01')}}, {'test_date' : {$lte : new ISODate('2005-03-31')}}]}, {'test_type.type_code' : 'NT'}]}},
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
    {$match : {$and : [{$and : [{'test_date' : {$gt : new ISODate('2004-01-01')}}, {'test_date' : {$lte : new ISODate('2005-12-31')}}]}, {'test_class_id' : 7}, {'outcome.result_code' : {$in: ['F', 'PRS' ]}}]}},
    {$project : {'test_id': 1, 'test_items': 1, '_id': 0}},
    {$unwind : '$test_items'},
    {$match : {'test_items.rfr_type_code' : {$in : ['F', 'P']}}},
    {$project : {'test_id': 1, 'item_name' : '$test_items.test_detail.item_group_name'}},
    {$group : {
            _id : '$item_name',
            'rfr_volume' : {$sum : 1},
            'test_volume_set' : {$addToSet  : '$test_id'} //use a set to hold distinct values
        }
    },
    {$project : {'item_name' : '$_id', 'rfr_volume' : 1, 'test_volume' : {$size : '$test_volume_set'}, '_id': 0}}
])




// -- Basic Expansion of RfR Hierarchy for Class 5 Vehicles
// -- ==================
db.item_detail.find({'test_class_id' : 5},
	{'_id' : -1,
	 'rfr_id' : 1,
	 'rfr_desc' : 1,
	 'item_group_level_1' : 1,
	 'item_group_level_2' : 1,
	 'item_group_level_3' : 1,
	 'item_group_level_4' : 1,
	 'item_group_level_5' : 1}
)