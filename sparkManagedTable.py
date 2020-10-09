import sys
from lib.readDataFrames import get_spark_app_config, load_rating_csv_df
from lib.writeDataFrames import write_spark_managed_table
from pyspark.sql import SparkSession
from lib.logger import Log4j
from pyspark.sql.functions import spark_partition_id

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting the pyspark application")

    logger.info("Reading data from csv files")
    ratingCsvDf = load_rating_csv_df(spark)
    ratingCsvDf.show(5)
    logger.info("Csv Schema: " + ratingCsvDf.schema.simpleString())

    logger.info("Creating the managed table from spark")
    write_spark_managed_table(spark,ratingCsvDf)
    logger.info(spark.catalog.listTables("RATINGS_DB"))

    logger.info("Completing the pyspark application")
