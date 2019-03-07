from __future__ import print_function
from pyspark.sql import SparkSession
from spark_utils import spark_utils

def create_spark_session(app_name="SparkApplication"):
    sc = spark_utils.generate_spark_context(20)
    spark = SparkSession(sc)

    spark.sparkContext.setLogLevel("WARN")

    return spark
