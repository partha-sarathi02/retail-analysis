from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_conf

def get_spark_session(env):
    if env=="LOCAL":
        return SparkSession.builder \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .config(conf=get_pyspark_conf(env)) \
            .master("local[2]") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=get_pyspark_conf(env)) \
            .enableHiveSupport() \
            .getOrCreate()