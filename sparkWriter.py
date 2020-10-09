import sys
from lib.readDataFrames import get_spark_app_config
from lib.readDataFrames import load_movie_csv_df
from lib.writeDataFrames import write_movie_df, write_movie_json_df
from pyspark.sql import SparkSession
from lib.logger import Log4j
from pyspark.sql.functions import spark_partition_id

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting the pyspark application")

    logger.info("Reading data from csv files")
    movieCsvDf = load_movie_csv_df(spark)
    movieCsvDf.show(5)
    logger.info("Csv Schema: " + movieCsvDf.schema.simpleString())

    logger.info("Writing avro data to output path")
    logger.info("Number of partitions before :" + str(movieCsvDf.rdd.getNumPartitions()))
    movieCsvDf.groupBy(spark_partition_id()).count().show()

    partitionDf = movieCsvDf.repartition(5)
    logger.info("Number of partitions after :" + str(partitionDf.rdd.getNumPartitions()))
    partitionDf.groupBy(spark_partition_id()).count().show()
    write_movie_df(partitionDf)
    write_movie_json_df(partitionDf)
    logger.info("Completing the pyspark application")
