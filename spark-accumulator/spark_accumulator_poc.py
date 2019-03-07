from pyspark import SparkContext, AccumulatorParam
from pyspark.serializers import CloudPickleSerializer
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
from pyspark.sql.functions import lit, pandas_udf, PandasUDFType, to_json
logger = logging.getLogger(__name__)

class SAP(AccumulatorParam):
    def zero(self, initialValue):
        s=set()
        s.add(initialValue)
        return s
    def addInPlace(self, v1, v2):

        return v1.union(v2)

if __name__ == '__main__':
    sc = SparkContext('local[1]', 'app', serializer=CloudPickleSerializer())
    spark = SparkSession(sc)
    from pyspark.sql import Row

    l = [('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)]
    rdd = sc.parallelize(l)
    people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
    schemaPeople = spark.createDataFrame(people)
    schemaPeople = schemaPeople.repartition(20)
    schema = StructType([StructField('name', StringType()),
                         StructField('age', IntegerType()), ])
    schemaPeople.show()


    def foo(pd):
        logger.debug('X' * 10 + 'debug' + 'X' * 10)
        logger.info('X' * 10 + 'info' + 'X' * 10)
        return pd.append(pd)


    doubled_df = schemaPeople.groupBy('age').apply(
        pandas_udf(lambda pd: foo(pd), schema, PandasUDFType.GROUPED_MAP))
    logger.warn(str(doubled_df.collect()))
