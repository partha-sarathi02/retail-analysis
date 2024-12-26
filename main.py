from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("creating spark session")
    spark= SparkSession.builder \
    .appName("streaming application") \
    .master("local[2]") \
    .getOrCreate()

#read the streaming data
    lines= spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port",9999) \
    .load()
#processing logic

#write to sink
    query= lines.writeStrem \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation","checkpointdir") \
    .start()

query.awaitTermination()