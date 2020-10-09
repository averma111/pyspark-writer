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


def write_movie_df(dataFrame):
    dataFrame.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "target/avro") \
        .save()


def write_movie_json_df(dataFrame):
    dataFrame.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "target/json/") \
        .partitionBy("genres") \
        .option("maxRecordsPerFile", "4000") \
        .save()
