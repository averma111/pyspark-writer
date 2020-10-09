from lib.schema import objectSchemaMovie
from pyspark import SparkConf
import configparser


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("config/spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def write_spark_managed_table(spark,dataFrame):
    spark.sql("CREATE DATABASE IF NOT EXISTS RATINGS_DB")
    spark.catalog.setCurrentDatabase("RATINGS_DB")

    dataFrame.write \
        .mode("overwrite") \
        .bucketBy(5, "genres") \
        .sortBy("genres") \
        .saveAsTable("Ratings_Tbl")
