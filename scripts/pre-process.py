import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# HDFS_NAMENODE = os.environ['CORE_CONF_fs_defaultFS']
CROPLAND_FIRES_URI = "hdfs://namenode:9000/cropland-fires"

SOURCE_DIR_PATH = CROPLAND_FIRES_URI + "/raw-data"
TARGET_DIR_PATH = CROPLAND_FIRES_URI + "/result/pre-process"

conf = SparkConf().setAppName("pre-processing").setMaster("spark://spark-master:7077")
session = SparkSession.builder.config(conf=conf).getOrCreate()
cropland_fires = session.read \
    .option("delimiter", ",") \
    .csv("{}/CroplandFiresEmissions.csv".format(SOURCE_DIR_PATH), header=True, inferSchema=True) \

df = cropland_fires.select("iso3_country", "original_inventory_sector", "start_time", "end_time",
                           "temporal_granularity", "gas", "emissions_quantity", "emissions_factor", "st_astext")

regex_pattern = r'POLYGON\(\(([-\d.]+ [-\d.]+,[-\d.]+ [-\d.]+,[-\d.]+ [-\d.]+,[-\d.]+ [-\d.]+,[-\d.]+ [-\d.]+)\)\)'

df = df.withColumn("emissions_quantity", round(df.emissions_quantity, 3)) \
    .withColumn("year", year(to_timestamp('start_time', 'yyyy-MM-dd HH:mm:ss'))) \
    .withColumn("month", month(to_timestamp('start_time', 'yyyy-MM-dd HH:mm:ss'))) \
    .withColumnRenamed("original_inventory_sector", "cause")\
    .withColumnRenamed("st_astext", "polygon_coordinates") \
    .withColumnRenamed("iso3_country", "alpha_3") \
    .withColumn("polygon_coordinates", regexp_extract("polygon_coordinates", regex_pattern, 1)) \
    .drop("start_time", "end_time")\

coord_list = split(df["polygon_coordinates"], ",")
for i in range(5):
    df = df.withColumn("coordinates_{}".format(i+1), coord_list[i])

result_df = df.drop("polygon_coordinates") \
    .withColumn("alpha_3", regexp_replace("alpha_3", "XKX", "SRB"))

result_df.coalesce(5).write \
    .mode(saveMode="overwrite") \
    .csv(TARGET_DIR_PATH, header=True)
