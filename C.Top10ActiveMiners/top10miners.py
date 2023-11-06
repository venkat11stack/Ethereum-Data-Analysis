import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    # Create a Spark session
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    # Set up environment variables for accessing S3 bucket
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    # Configure Hadoop to access S3 bucket
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Read blocks data from S3 bucket
    blocks = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")

    # Filter valid blocks (ignoring header and checking the number of fields)
    valid_blocks = blocks.filter(lambda line: len(line.split(',')) == 19 and line.split(',')[1] != 'hash')

    # Extract miner and size from the valid blocks
    miner_size_tuples = valid_blocks.map(lambda line: (line.split(',')[9], int(line.split(',')[12])))

    # Aggregate block sizes for each miner
    aggregated_miners = miner_size_tuples.reduceByKey(lambda size1, size2: size1 + size2)

    # Retrieve the top 10 miners by block size
    top10_miners = aggregated_miners.takeOrdered(10, key=lambda entry: -entry[1])

    # Set up S3 bucket resource for storing the results
    my_bucket_resource = boto3.resource('s3',
                                        endpoint_url='http://' + s3_endpoint_url,
                                        aws_access_key_id=s3_access_key_id,
                                        aws_secret_access_key=s3_secret_access_key)

    # Get the current date and time for the output filename
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H_%M_%S")

    # Construct the output file name with the date and time
    output_file_name = f"top10miners_{date_time}.txt"

    # Save the top 10 miners to the S3 bucket
    my_result_object = my_bucket_resource.Object(s3_bucket, output_file_name)
    my_result_object.put(Body=json.dumps(top10_miners))

    # Stop the Spark session
    spark.stop()
