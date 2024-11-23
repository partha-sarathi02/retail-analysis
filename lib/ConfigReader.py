import configparser
from pyspark import SparkConf

#loading application config in python dictionary
def get_app_config(env):
    config= configparser.ConfigParser()
    config.read("configs/application.conf")
    app_conf= {}
    for (key,value) in config.items(env):
        app_conf[key]=value
    return app_conf

#loading pyspark config and creating a spark conf object
def get_pyspark_conf(env):
    config= configparser.ConfigParser()
    config.read("configs/pyspark.conf")
    pyspark_conf= SparkConf()
    for (key,value) in config.items(env):
        pyspark_conf.set(key,value)
    return pyspark_conf