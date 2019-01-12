wget http://data.dft.gov.uk/anonymised-mot-test/test_data/test_result_2005.txt.gz
wget http://data.dft.gov.uk/anonymised-mot-test/test_data/test_item_2005.txt.gz

wget http://data.dft.gov.uk/anonymised-mot-test/lookup.zip

unzip lookup.zip
mkdir lookup

mv *.txt lookup

gunzip test_result_2005.txt.gz
gunzip test_item_2005.txt.gz

hdfs dfs -mkdir -p /user/maria_dev/mot_data/test_item
hdfs dfs -mkdir -p /user/maria_dev/mot_data/test_result
hdfs dfs -mkdir -p /user/maria_dev/mot_data/item_details
hdfs dfs -mkdir -p /user/maria_dev/mot_data/item_group
hdfs dfs -mkdir -p /user/maria_dev/mot_data/lookup/mdr_fuel_types
hdfs dfs -mkdir -p /user/maria_dev/mot_data/lookup/mdr_rfr_location
hdfs dfs -mkdir -p /user/maria_dev/mot_data/lookup/mdr_test_outcome
hdfs dfs -mkdir -p /user/maria_dev/mot_data/lookup/mdr_test_types

hdfs dfs -moveFromLocal test_result_2005.txt  /user/maria_dev/mot_data/test_result/
hdfs dfs -moveFromLocal test_item_2005.txt  /user/maria_dev/mot_data/test_item/
hdfs dfs -moveFromLocal lookup/item_detail.txt  /user/maria_dev/mot_data/lookup/item_details/
hdfs dfs -moveFromLocal lookup/item_group.txt  /user/maria_dev/mot_data/lookup/item_group/
hdfs dfs -moveFromLocal lookup/mdr_fuel_types.txt  /user/maria_dev/mot_data/lookup/mdr_fuel_types/
hdfs dfs -moveFromLocal lookup/mdr_rfr_location.txt  /user/maria_dev/mot_data/lookup/mdr_rfr_location/
hdfs dfs -moveFromLocal lookup/mdr_test_outcome.txt  /user/maria_dev/mot_data/lookup/mdr_test_outcome/
hdfs dfs -moveFromLocal lookup/mdr_test_types.txt  /user/maria_dev/mot_data/lookup/mdr_test_types/



