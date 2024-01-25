import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import *

# HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
# HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]
HIVE_METASTORE_URIS = "thrift://hive-metastore:9083"
CROPLAND_FIRES_URI = "hdfs://namenode:9000/cropland-fires"

SOURCE_DIR_PATH = CROPLAND_FIRES_URI + "/result/pre-process"
TARGET_DIR_PATH = CROPLAND_FIRES_URI + "/result"

conf = SparkConf()\
    .setAppName("time-dims")\
    .setMaster("spark://spark-master:7077")\
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .set("spark.sql.warehouse.dir", "/hive/warehouse") \
    .set("hive.metastore.uris", HIVE_METASTORE_URIS)

session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
main_df = session.read \
    .option("delimiter", ",") \
    .csv("{}".format(SOURCE_DIR_PATH), header=True, inferSchema=True)

# Year dimension (distinct() no needed???)
year_df = main_df.groupBy("year") \
    .agg(round(sum("emissions_quantity"), 3).alias("total_gas_emission"))\
    .distinct() \
    .withColumn("id", monotonically_increasing_id()) \
    .withColumnRenamed("year", "value") \
    .orderBy(col("value").asc())

year_df.coalesce(1).write \
    .mode(saveMode="overwrite") \
    .csv("{}/year".format(TARGET_DIR_PATH), header=True)

year_df.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("year")

# Month dimension
month_df = main_df.select("month", "year")\
    .distinct() \
    .withColumn("id", monotonically_increasing_id()) \
    .withColumnRenamed("month", "num_value") \
    .withColumn("num_value", col("num_value").cast(StringType())) \
    .withColumn("name", date_format(to_date("num_value", "MM"), "MMMM"))

month_df = month_df.join(year_df.withColumnRenamed("id", "year_id"), month_df.year == year_df.value, how="left") \
    .withColumn("num_value", col("num_value").cast(IntegerType())) \
    .orderBy(col("year").asc(), col("num_value").asc()) \
    .drop("year", "value", "total_gas_emission") \

month_df.coalesce(1).write \
    .mode(saveMode="overwrite") \
    .csv("{}/month".format(TARGET_DIR_PATH), header=True)

month_df.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("month")
