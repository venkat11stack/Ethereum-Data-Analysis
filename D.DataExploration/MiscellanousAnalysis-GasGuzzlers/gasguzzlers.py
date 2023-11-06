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
    
    # Read transactions and contracts data from S3 bucket
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    
     # Filter valid transactions and contracts
    valid_transactions = transactions.filter(lambda line: len(line.split(',')) == 15 and line.split(',')[9].replace('.', '', 1).isdigit() and line.split(',')[11].replace('.', '', 1).isdigit())
    valid_contracts = contracts.filter(lambda line: len(line.split(',')) == 6)
    
    # Calculate average gas price
    avg_gas_price = valid_transactions.map(lambda line: (time.strftime("%m/%Y", time.gmtime(int(line.split(',')[11]))), (float(line.split(',')[9]), 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .sortByKey(ascending=True) \
        .map(lambda a: (a[0], str(a[1][0] / a[1][1])))

    # Calculate average gas used for contract transactions
    trans_gas = valid_transactions.map(lambda line: (line.split(',')[6], (time.strftime("%m/%Y", time.gmtime(int(line.split(',')[11]))), float(line.split(',')[8]))))
    contract_flag = valid_contracts.map(lambda x: (x.split(',')[0], 1))
    joined_data = trans_gas.join(contract_flag)
    avg_gas_used = joined_data.map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1]))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .map(lambda a: (a[0], str(a[1][0] / a[1][1]))) \
    .sortByKey(ascending=True)
    
    # Set up S3 bucket resource for storing the results
    my_bucket_resource = boto3.resource('s3',
                                        endpoint_url='http://' + s3_endpoint_url,
                                        aws_access_key_id=s3_access_key_id,
                                        aws_secret_access_key=s3_secret_access_key)

    # Get the current date and time for the output filename
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H_%M_%S")
    
    # Construct the output file name with the date and time
    output_file_name1 = f"avg_gas_price_{date_time}.txt"
    output_file_name2 = f"avg_gas_used_{date_time}.txt"

    # Write the output to a file in S3
    my_result_object = my_bucket_resource.Object(s3_bucket,output_file_name1 )
    my_result_object.put(Body=json.dumps(avg_gas_price.take(100)))
    my_result_object1 = my_bucket_resource.Object(s3_bucket,output_file_name2 )
    my_result_object1.put(Body=json.dumps(avg_gas_used.take(100)))

    # Stop the Spark session
    spark.stop()