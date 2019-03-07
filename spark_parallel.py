from pyspark import SparkContext
from pyspark.serializers import CloudPickleSerializer
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sc = SparkContext('local[1]', 'app', serializer=CloudPickleSerializer())
    spark = SparkSession(sc)
    raw = [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)]
    rdd = sc.parallelize(raw)
    data_frame = spark.createDataFrame(rdd)


    def foo(df1):
        df1.show(truncate=False)

    for df2 in [data_frame]:
        foo(df2)


    spark.stop()




