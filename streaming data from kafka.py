from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
import time
import json

# Define Spark Session
sc = SparkContext(appName='kafka-stream')
sc.setLogLevel('WARN')
spark = SparkSession(sc)
 
#Read streaming data from Kafka into Pyspark dataframe
df=spark.readStream.format('kafka').option('kafka.bootstrap.servers','localhost:9092') \
                                      .option('subscribe', 'kafka-topic').option("failOnDataLoss","false") \
                                      .option('startingOffsets', 'earliest').load().selectExpr("CAST(value AS STRING)")
df.printSchema()


#Define schema for the data
userSchema =StructType([
StructField('Global_new_confirmed',StringType()),
StructField('Global_new_deaths',StringType()),
StructField('Global_new_recovered',StringType()),
StructField('Global_total_confirmed',StringType()),
StructField('Global_total_deaths',StringType()),
StructField('Global_total_recovered',StringType()),
StructField('Country_code',StringType()),
StructField('Country_name',StringType()),
StructField('Country_new_deaths',StringType()),
StructField('Country_new_recovered',StringType()),
StructField('Country_newconfirmed',StringType()),
StructField('Country_slug',StringType()),
StructField('Country_total_confirmed',StringType()),
StructField('Country_total_deaths',StringType()),
StructField('Country_total_recovered',StringType()),
StructField('Extracted_timestamp',TimestampType())
])




#Parse the data 
def parse_data_from_kafka_message(sdf, schema):
  assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
  col = split(sdf['value'], ',') 
  # split attributes to nested array in one Column
  for idx, field in enumerate(schema): 
      sdf  = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
  return sdf.select([field.name for field in schema])

df = parse_data_from_kafka_message(df, userSchema)


#Process the data 
df_agg=df.groupBy("Country_code","Country_name","Country_total_deaths","Extracted_timestamp").count()

#Write streaming data to output Kafka topic which can be consumed by destination services like #HDFS, Nifi, etc.
df2=df_agg.select(to_json(struct(
'Country_code',
'Country_name',
'Country_total_deaths','Extracted_timestamp')).alias('value')).writeStream.format("kafka").outputMode("complete") \
                                              .option("failOnDataLoss","false").option('checkpointLocation','/home/ec2-user/checkpoint_out') \
                                              .option("kafka.bootstrap.servers","localhost:9092").option("topic", "Aggreagte-data-topic") \
                                              .start().awaitTermination()
