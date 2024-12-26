from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("creating spark session")
    spark= SparkSession.builder \
    .appName("streaming application") \
    .master("local[2]") \
    .getOrCreate()

#read the streaming data

    order_schema= 'order_id long, order_date date, order_customer_id long, order_status string'
    order_df= spark.readStream \
    .format("json") \
    .schema(order_schema) \
    .option("path", "/inputdir") \
    .load()
#processing logic
    order_df.createOrReplaceTempView("orders")
    completed_orders= spark.sql("select * from orders where order_status='COMPLETE'")

#write to sink
    query= completed_orders.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path","/outputdir") \
    .option("checkpointLocation","checkpointdir") \
    .start()
    
query.awaitTermination()
