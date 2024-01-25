import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
# HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]
HIVE_METASTORE_URIS = "thrift://hive-metastore:9083"
CROPLAND_FIRES_URI = "hdfs://namenode:9000/cropland-fires"

SOURCE_DIR_PATH = CROPLAND_FIRES_URI + "/raw-data"
TARGET_DIR_PATH = CROPLAND_FIRES_URI + "/result"

conf = SparkConf().setAppName("gas-dim").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
main_df = session.read \
    .option("delimiter", ",") \
    .csv("{}/gases.csv".format(SOURCE_DIR_PATH), header=True, inferSchema=True)

gases_df = main_df.withColumn("id", monotonically_increasing_id()+1) \
    .orderBy(col("chemical-label").asc())

gases_df.coalesce(1).write \
    .mode(saveMode="overwrite") \
    .csv("{}/gas".format(TARGET_DIR_PATH), header=True)

gases_df.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("gas")
