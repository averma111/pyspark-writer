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


def load_movie_csv_df(spark):
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(objectSchemaMovie()) \
        .option("mode", "FAILFAST") \
        .load("/home/ashish/ml-20m/movies.csv")



