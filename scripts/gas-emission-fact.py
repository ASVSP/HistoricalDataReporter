import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
# HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]
HIVE_METASTORE_URIS = "thrift://hive-metastore:9083"

RESULT_DIR_URI = "hdfs://namenode:9000/cropland-fires/result"
SOURCE_DIR_PATH = RESULT_DIR_URI + "/pre-process"
GAS_DIR_PATH = RESULT_DIR_URI + "/gas"
MONTH_DIR_PATH = RESULT_DIR_URI + "/month"
YEAR_DIR_PATH = RESULT_DIR_URI + "/year"
COUNTRY_DIR_PATH = RESULT_DIR_URI + "/country"

conf = SparkConf().setAppName("gas-emission-fact").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
source_df = session.read \
    .option("delimiter", ",") \
    .csv("{}".format(SOURCE_DIR_PATH), header=True, inferSchema=True)

gas_df = session.read \
    .option("delimiter", ",") \
    .csv("{}".format(GAS_DIR_PATH), header=True, inferSchema=True)

month_df = session.read \
    .option("delimiter", ",") \
    .csv("{}".format(MONTH_DIR_PATH), header=True, inferSchema=True)

year_df = session.read \
    .option("delimiter", ",") \
    .csv("{}".format(YEAR_DIR_PATH), header=True, inferSchema=True)

country_df = session.read \
    .option("delimiter", ",") \
    .csv("{}".format(COUNTRY_DIR_PATH), header=True, inferSchema=True)

time_df = month_df.select("num_value", "id", "year_id") \
    .withColumnRenamed("id", "month_id") \
    .join(year_df.withColumnRenamed("id", "year_id").select("year_id", "value"), ["year_id"], how="left")

result_df = source_df.select("alpha_3", "gas", "emissions_quantity", "year", "month") \
    .join(country_df.withColumnRenamed("code", "country_code"), source_df["alpha_3"] == country_df["alpha_3"], how="left") \
    .join(gas_df.withColumnRenamed("id", "gas_id"), source_df["gas"] == gas_df["chemical-label"], how="left") \
    .join(time_df, (source_df["month"] == time_df["num_value"]) & (source_df["year"] == time_df["value"]), how="left") \
    .select("country_code", "month_id", "gas_id", "emissions_quantity")

result_df = result_df.withColumnRenamed("emissions_quantity", "quantity") \
    .withColumn("id", monotonically_increasing_id()+1)

result_df.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("gas_emission")
