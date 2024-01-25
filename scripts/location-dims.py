import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
# HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]
HIVE_METASTORE_URIS = "thrift://hive-metastore:9083"
CROPLAND_FIRES_URI = "hdfs://namenode:9000/cropland-fires"

SOURCE_DIR_PATH1 = CROPLAND_FIRES_URI + "/raw-data"
SOURCE_DIR_PATH2 = CROPLAND_FIRES_URI + "/result/pre-process"
TARGET_DIR_PATH = CROPLAND_FIRES_URI + "/result"

conf = SparkConf().setAppName("location-dims").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
country_region = session.read \
    .option("delimiter", ",") \
    .csv("{}/country-region.csv".format(SOURCE_DIR_PATH1), header=True, inferSchema=True)

pre_processed = session.newSession().read \
    .option("delimiter", ",") \
    .csv("{}".format(SOURCE_DIR_PATH2), header=True, inferSchema=True) \
    .select("alpha_3", "emissions_quantity")

# Total emission quantity
temp_df = pre_processed.join(country_region, pre_processed["alpha_3"] == country_region["alpha-3"], how="left") \
    .select("country-code", "sub-region-code", "emissions_quantity", "alpha_3")

total_emissions_country_df = temp_df.groupBy("country-code") \
    .agg(round(sum("emissions_quantity"), 3).alias("total_gas_emission"))

total_emissions_region_df = temp_df.groupBy("sub-region-code") \
    .agg(round(sum("emissions_quantity"), 3).alias("total_gas_emission")) \
    .na.drop(subset=["sub-region-code"])

# Pre-process
country_region = country_region.drop("iso_3166-2", "intermediate-region", "intermediate-region-code") \
    .withColumnRenamed("alpha-2", "alpha_2") \
    .withColumnRenamed("alpha-3", "alpha_3") \
    .withColumnRenamed("country-code", "country_code")

# Country
country_df = country_region.select("country_code", "name", "alpha_2", "alpha_3", "sub-region-code") \
    .withColumnRenamed("country_code", "code") \
    .withColumnRenamed("sub-region-code", "region_code")

country_df = country_df.join(total_emissions_country_df, country_df["code"] == total_emissions_country_df["country-code"], how="left") \
    .drop("country-code")

country_df.coalesce(1).write \
    .mode(saveMode="overwrite") \
    .csv(TARGET_DIR_PATH + "/country", header=True)

country_df.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("country")

# Region
region_df = country_region.select("sub-region-code", "sub-region", "region-code")\
    .distinct()\
    .na.drop(how="all") \
    .withColumnRenamed("sub-region-code", "code") \
    .withColumnRenamed("sub-region", "name") \
    .withColumnRenamed("region-code", "continent_code")

region_df = region_df.join(total_emissions_region_df, region_df["code"] == total_emissions_region_df["sub-region-code"], how="left") \
    .drop("sub-region-code")

region_df.coalesce(1).write \
    .mode(saveMode="overwrite") \
    .csv(TARGET_DIR_PATH + "/region", header=True)

region_df.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("region")

# Continent
continent_df = country_region.select("region-code", "region")\
    .distinct()\
    .na.drop("all") \
    .withColumnRenamed("region-code", "code") \
    .withColumnRenamed("region", "name")

continent_df.coalesce(1).write \
    .mode(saveMode="overwrite") \
    .csv(TARGET_DIR_PATH + "/continent", header=True)

continent_df.write \
    .mode(saveMode="overwrite") \
    .saveAsTable("continent")
