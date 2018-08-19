
wget https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD 

hdfs dfs -mkdir -p ./rawdata/chicago/crime
hdfs dfs -put Crimes_-_2001_to_present.csv ./rawdata/chicago/crime

