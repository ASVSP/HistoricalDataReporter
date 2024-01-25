cd ./asvsp/data || exit 1
/opt/hadoop-3.2.1/bin/hdfs dfs -rm -r -f /cropland-fires*
/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir /cropland-fires
/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir /cropland-fires/raw-data
/opt/hadoop-3.2.1/bin/hdfs dfs -put ./cropland-fires/CroplandFiresEmissions.csv /cropland-fires/raw-data
/opt/hadoop-3.2.1/bin/hdfs dfs -put ./cropland-fires/country-region.csv /cropland-fires/raw-data
/opt/hadoop-3.2.1/bin/hdfs dfs -put ./cropland-fires/gases.csv /cropland-fires/raw-data