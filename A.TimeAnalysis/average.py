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
    
    # Read the dataset from the S3 bucket
    t = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    
    # Filter out invalid lines
    clean_lines = t.filter(lambda line: len(line.split(',')) == 15 and all([line.split(',')[7].replace(".", "", 1).isdigit(), line.split(',')[11].isdigit()]))

    # Process the lines to extract the date and transaction value
    date_vals = clean_lines.map(lambda line: (time.strftime("%m/%Y", time.gmtime(int(line.split(',')[11]))), (float(line.split(',')[7]), 1)))

    # Function to combine the transaction values and their counts
    def combine_values(x, y):
        return (x[0] + y[0], x[1] + y[1])

    # Aggregate the transaction values and counts for each month/year
    reducing = date_vals.reduceByKey(combine_values)

    # Function to calculate the average transaction value for each month/year
    def calculate_average(record):
        date, values = record
        total_value, count = values
        avg_value = total_value / count
        return date, str(avg_value)

    # Calculate the average transaction value for each month/year
    avg_transactions = reducing.map(calculate_average)
    
    # Convert the tuples to strings
    avg_transactions = avg_transactions.map(lambda op: ','.join(str(tr) for tr in op))

    # Configure the S3 resource for writing the output
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    # Get the current date and time for the output filename
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H_%M_%S")
    
    # Construct the output file name with the date and time
    output_file_name = f"avg_transactions_{date_time}.txt"
    
    # Write the output to a file in S3
    my_result_object = my_bucket_resource.Object(s3_bucket,output_file_name)
    my_result_object.put(Body=json.dumps(avg_transactions.take(100)))
    
    spark.stop()