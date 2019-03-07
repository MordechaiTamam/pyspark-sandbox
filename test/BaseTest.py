from pyspark.sql import SparkSession


class BaseTest:

    import datetime;
    ts = datetime.datetime.now()
    # spark_session = SparkSession.builder \
    #     .appName("test-{0}".format(ts)) \
    #     .master("local[*]") \
    #     .getOrCreate()
    #
    # spark_session.sparkContext.setLogLevel("WARN")
    # spark = spark_session
