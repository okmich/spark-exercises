# command to run the mot-mongo main class
# change the hdfs path to change the file you are loading
spark-submit --master yarn --class app.Main --conf spark.sql.shuffle.partitions=300 ./mot-mongo-assembly-0.1.0-SNAPSHOT.jar -test_result_dir=/user/maria_dev/mot_data/test_result/f2010 -test_item_dir=/user/maria_dev/mot_data/test_item/f2010 -lookup_data_dir=/user/maria_dev/mot_data/lookup -mongoserver=192.168.8.104




# command to run the mot-cassandra main class
spark-submit --master yarn --class app.Main mot-cassandra-assembly-0.1.0-SNAPSHOT.jar -test_result_dir=[hdfspath] -test_item_dir=[hdfspath] -lookup_data_dir=[hdfspath] -cassandra_host=[ip] -model=[model_to_load]