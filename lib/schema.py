from pyspark.sql.dataframe import StructType, StructField, IntegerType, StringType


def objectSchemaMovie():
    return StructType([
        StructField("movieId", IntegerType()),
        StructField("title", StringType()),
        StructField("genres", StringType())
    ])
