import sys
from lib.readDataFrames import get_spark_app_config
from lib.readDataFrames import load_csv_df
from lib.readDataFrames import load_json_df
from lib.readDataFrames import load_parquet_df
from lib.readDataFrames import load_movie_csv_df
from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting the pyspark application")

    movieCsvDf = load_movie_csv_df(spark)
    movieCsvDf.show(5)
    logger.info("Csv Schema: " + movieCsvDf.schema.simpleString())
   # flightCsvDf = load_csv_df(spark)
   # flightCsvDf.show(5)
  #  logger.info("Csv Schema: " + flightCsvDf.schema.simpleString())

   # flightJsonDf = load_json_df(spark)
  #  flightJsonDf.show(5)
   # logger.info("Json Schema: " + flightJsonDf.schema.simpleString())

    #flightParquetDf = load_parquet_df(spark)
    #flightParquetDf.show(5)

    logger.info("Completing the pyspark application")
