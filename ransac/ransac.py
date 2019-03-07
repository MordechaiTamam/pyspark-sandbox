import os
import sys

# Path for spark source folder
import time
from ransac_init import LinearExample, ransacSpark,ransac

def ransacStep(mapCreationProgram):
    try:
        sc=mapCreationProgram.sc
        from pyspark import SparkContext
        from pyspark import SparkConf

        print("initialized spark context")

        linearExample = LinearExample()
        samples = linearExample.initSamplesForRansac()
        startTime = time.time()

        result = ransac(samples,30000,linearExample.createModelFromSamples, linearExample.scoreModelAgainstSamples)

        print("calculated RANSAC took " + str(time.time() - startTime))
        print("\n FINAL RESULT: {0}".format(result))
        startTime = time.time()
        result2 = ransacSpark(samples, 30000, linearExample.modelFromSamplePair, linearExample.scoreModelAgainstSamples, sc)
        print("calculated RANSAC SPARK took " + str(time.time() - startTime))
        print("\n FINAL RESULT: ")
        print(result2)
    except ImportError as e:
        print ("Can not import Spark Modules", e)