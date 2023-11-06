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

    # Read environment variables for S3 configuration
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    # Configure Hadoop to access S3
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Read the transactions file from S3
    t = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")

    # Filter out invalid lines
    clean_lines = t.filter(lambda line: len(line.split(',')) == 15 and line.split(',')[11].isdigit())

    # Process the lines to get the count of transactions per month/year
    transactions = clean_lines.map(lambda line: (time.strftime("%m/%Y", time.gmtime(int(line.split(',')[11]))), 1))
    total_transactions = transactions.reduceByKey(lambda a, b: a + b)

    # Configure the S3 resource for writing the output
    my_bucket_resource = boto3.resource('s3',
                                        endpoint_url='http://' + s3_endpoint_url,
                                        aws_access_key_id=s3_access_key_id,
                                        aws_secret_access_key=s3_secret_access_key)
    
    # Get the current date and time for the output filename
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H_%M_%S")
    
    # Construct the output file name with the date and time
    output_file_name = f"transactions_total_{date_time}.txt"
    
    # Write the output to a file in S3
    my_result_object = my_bucket_resource.Object(s3_bucket, output_file_name)
    my_result_object.put(Body=json.dumps(total_transactions.take(100)))

    # Print the top 100 records to the console
    print(total_transactions.take(100))

    # Stop the Spark session
    spark.stop()
