import os
import unittest

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from ransac.ransac_init import LinearExample, ransac, ransacSpark
from test.BaseTest import BaseTest


class TestStringMethods(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(TestStringMethods,self).__init__(methodName)
        import datetime;
        ts = datetime.datetime.now()
        # os.environ["PYSPARK_PYTHON"] = "/home/modi/.conda/envs/py27/bin/"
        # conf.setExecutorEnv("PYTHONPATH", "$PYTHONPATH:" +
        #                     os.path.abspath(
        #                         os.path.join(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir),
        #                                      os.pardir)))
        # spark_session = SparkSession.builder \
        #     .appName("test-{0}".format(ts)) \
        #     .master("local[*]") \
        #     .config("PYTHONPATH", "$PYTHONPATH:" +
        #                     os.path.abspath(
        #                         os.path.join(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir),
        #                                      os.pardir)))\
        #     .getOrCreate()
        #
        # spark_session.sparkContext.setLogLevel("WARN")
        # self._spark = spark_session

    def test_ransac(self):
        linear_example = LinearExample()
        samples = linear_example.initSamplesForRansac()
        result = ransac(samples, 30000, linear_example.createModelFromSamples,
                        linear_example.scoreModelAgainstSamples)
        print("\n FINAL RESULT: {0}".format(result))


    def test_ransac_spark(self):
        conf = SparkConf()
        conf.set("spark.executor.memory", "1g")
        conf.set("spark.cores.max", "1")
        conf.set("spark.app.name", "nosetest")
        sc = SparkContext(conf=conf)

        linear_example = LinearExample()
        samples = linear_example.initSamplesForRansac()
        result = ransacSpark(samples, 30000, linear_example.modelFromSamplePair,
                             linear_example.scoreModelAgainstSamples, sc)
        print("\n FINAL RESULT: {0}".format(result))


if __name__ == '__main__':
    unittest.main()
