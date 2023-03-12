# Real-time-data-analytics-using-kafka-on-AWS

# Real-time-data-analytics-using-kafka-on-AWS

In this Project I Simulated a real time covid 19 data from https://api.covid19api.com/summary API.

The data is filtered and cleaned using pandas and then sent to Kafka topic setup in AWS EC2.
The data is read from the Kafka topic using Spark Streaming ReadStream API. It isaggregated by calculating the stats of the columns and pushed to different Kafka topic.

The aggregated real time data is pushed to S3 using Boto3 SDK Library in Json format for furthur analysis.

The data is crawled over using AWS Glue Crawler on S3 location and data is queried using AWS Athena for Data Analytics.
