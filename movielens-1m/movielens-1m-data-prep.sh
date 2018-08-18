wget http://files.grouplens.org/datasets/movielens/ml-1m.zip

unzip ml-1m.zip

hdfs dfs -mkdir -p ./rawdata/movielens/1m/movies
hdfs dfs -mkdir -p ./rawdata/movielens/1m/users
hdfs dfs -mkdir -p ./rawdata/movielens/1m/ratings
hdfs dfs -mkdir -p ./rawdata/movielens/1m/age
hdfs dfs -mkdir -p ./rawdata/movielens/1m/occupation

hdfs dfs -moveFromLocal ml-1m/movies.dat ./rawdata/movielens/1m/movies/
hdfs dfs -moveFromLocal ml-1m/ratings.dat ./rawdata/movielens/1m/ratings/
hdfs dfs -moveFromLocal ml-1m/users.dat ./rawdata/movielens/1m/users/

#hdfs dfs -moveFromLocal age.dat ./rawdata/movielens/1m/age/
#hdfs dfs -moveFromLocal occupation.dat ./rawdata/movielens/1m/occupation/
