from __future__ import print_function
import logging
import sys
from random import random
from pyspark.sql.types import *
from pyspark.sql.functions import col, pandas_udf, PandasUDFType

cSchema = StructType([StructField("v", IntegerType())])
spark_conf_dict = {
    "spark.driver.maxResultSize": "20g",
    "spark.python.worker.memory": "4g",
    "total-executor-cores": 1000,
    "fs.s3a.connection.maximum": "1000",
    "parallelism-to-tasks-factor": 4,
    "spark.worker.cleanup.enabled": "true",
    "spark.worker.cleanup.interval": 600,
    # "spark.driver.extraClassPath": "/opt/spark/out_of_classpath/log4j-2/*",
    # "spark.driver.extraJavaOptions": "-Dapp_name=my_prog_1446 -Dlog4j.configurationFile=file:/opt/spark/out_of_classpath/log4j-2/log4j2.xml -Dlogzio.level=WARN -Dlog.level=WARN",
    # "spark.executor.extraJavaOptions": '-Dapp_name=my_prog_1446 -Dexecutor.type=worker -Dlog4j.configurationFile=file:/opt/spark/out_of_classpath/log4j-2/log4j2.xml -Dlogzio.level=WARN -Dlog.level=WARN',
    # "spark.executor.extraClassPath": "/opt/spark/out_of_classpath/log4j-2/*",
}


class Program:
    def __init__(self, spark):
        self.spark = spark

    def run(self):
        self.generate_df()

    def generate_df(self):
        import random
        import numpy as np
        my_randoms = []
        pow = 5
        count = 5000
        for i in range(count):
            my_randoms.append(np.random.randint(10 ** (pow + 2), size=1).tolist())

        df = self.spark.createDataFrame(my_randoms, cSchema)
        schema = StructType([
            StructField('v', IntegerType()),
            StructField('v1', IntegerType())
        ])

        @pandas_udf(functionType=PandasUDFType.GROUPED_MAP, returnType=schema)
        def plus_one(v):
            import pandas as pd
            _count = 1000
            string = v.to_string()
            initial_v_len = len(v)
            v = v.append([v] * (_count - 1), ignore_index=True)
            tolist = np.random.randint(_count, size=_count).tolist()
            try:
                v['v1'] = pd.Series(tolist).values
            except ValueError as value_err:
                type, value, traceback = sys.exc_info()
                raise ValueError, (
                " len of v:{}, initial_v_len: {}, v:{}".format(len(v), initial_v_len, string), type, value), traceback
            import time
            time.sleep(600)
            return v

        sc = self.spark.sparkContext
        log4jLogger = sc._jvm.org.apache.log4j
        log = log4jLogger.LogManager.getLogger(__name__)
        log.warn("Hello World!")
        log.warn('About to start...')
        df = df.distinct().groupBy('v').apply(plus_one)

        df.repartition(100).write.mode('overwrite').parquet('s3a://rem-spark-staging/rem-dev/rem/users/modit/test')

    def method_name(self):
        sc = self.spark.sparkContext
        total_points = 1000000
        numbers = sc.range(0, total_points)
        points = numbers.map(lambda n: (random(), random()))
        circle = points.filter(lambda p: (p[0] * p[0] + p[1] * p[1]) < 1)
        num_inside = circle.count()
        print("Pi = ", 4 * num_inside / total_points)


if __name__ == '__main__':
    import os
    import getpass
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--spark_run", action="store_true", help="run the application on cloud",
                        default=False)  # TODO remove and make automatic
    parser.add_argument("--created_by_user", help="the user this run is owned by", default=getpass.getuser())
    parser.add_argument("--use_logzio", help="toggle logzio", action="store_true")
    parser.add_argument("--app_name", help="Used by spark?")
    parser.add_argument("--git_log_path", help="(cloud runs only) path to git log txt file", default=None)
    parser.add_argument("--git_status_path", help="(cloud runs only) path to git stats txt file", default=None)
    parser.add_argument("--git_diff_path", help="(cloud runs only) path to git diff txt file", default=None)

    args = parser.parse_args()
    if args.spark_run:
        abspath = os.path.abspath(__file__)
        components = abspath.split(os.sep)
        mepy_algo_path = str.join(os.sep, components[:components.index("sandbox") + 1])
        user = getpass.getuser()
        from spark_utils import spark_run as spark_run
        from spark_utils.spark_run import livyRC

        cluster = "admin.rem-dev.remdevme.com"
        livy_rc = livyRC(host_name=cluster)
        # cluster = "localhost"
        spark_run_args = spark_run.get_args(args=[
            '--run', abspath,
            '--pyFiles', '/localhomes/modit/dev/dev/spark_utils',
            '--livy_hostname', cluster,
            '--args', "created_by_user={}".format(user)
        ])
        spark_run_args.raw = True
        spark_conf_dict = {
            "spark.driver.extraJavaOptions": "-Dlog4j.configurationFile=file:/opt/spark/out_of_classpath/log4j-2/log4j2.xml -Dlogzio.level=DEBUG -Dlog.level=INFO",
            "spark.driver.extraClassPath": "/opt/spark/out_of_classpath/log4j-2/*",
        }
        spark_run.wrap_and_run(args=spark_run_args,rc=livy_rc)
        logging.info('done...')
    else:
        from datetime import datetime
        import time

        time.time()

        import spark_utils.spark_utils as spark_utils


        def create_spark_session(app_name="SparkApplication_{}".format(
            datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))):
            def init_calc_props(spark_ctx_conf):
                if {'parallelism-to-tasks-factor', 'total-executor-cores'} <= set(dict(spark_ctx_conf.getAll())):
                    total_executor_cores = spark_utils.calc_max_cores(int(spark_ctx_conf.get('total-executor-cores')))
                    parallelism_to_tasks_factor = int(spark_ctx_conf.get('parallelism-to-tasks-factor'))
                    value = str(total_executor_cores * parallelism_to_tasks_factor)
                    logging.info(
                        "total_executor_cores: {0}, parallelism_to_tasks_factor: {1},value: {2}".format(
                            total_executor_cores,
                            parallelism_to_tasks_factor,
                            value))
                    spark_ctx_conf.set('spark.default.parallelism', value)
                    spark_ctx_conf.set('spark.sql.shuffle.partitions', value)
                    logging.debug('Starting local spark with conf: {0}'.format(
                        "\n".join(str(v) for v in spark_ctx_conf.getAll())))

            from pyspark.sql import SparkSession
            from pyspark.context import SparkConf
            spark_conf = SparkConf()
            spark_conf.setAll(spark_conf_dict.items())
            init_calc_props(spark_conf)
            sc = spark_utils.generate_spark_context(spark_conf=spark_conf)
            spark = SparkSession(sc)

            spark.sparkContext.setLogLevel("WARN")

            return spark


        program = Program(create_spark_session())
        program.run()
