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

    # Filter valid transactions and contracts data
    valid_transactions = transactions.filter(lambda line: len(line.split(',')) == 15 and line.split(',')[3].isdigit())
    valid_contracts = contracts.filter(lambda line: len(line.split(',')) == 6)

    # Create (address, value) tuples from the valid transactions
    addr_val_tuples = valid_transactions.map(lambda tx: (tx.split(',')[6], int(tx.split(',')[7])))

    # Create (address, 1) tuples from the valid contracts
    contract_addr_tuples = valid_contracts.map(lambda contract: (contract.split(',')[0], 1))

    # Aggregate transaction values for each address
    agg_transactions = addr_val_tuples.reduceByKey(lambda val1, val2: val1 + val2)

    # Merge the aggregated transaction values with contract addresses
    merged_transactions_contracts = agg_transactions.join(contract_addr_tuples)

    # Extract the address and corresponding transaction value
    addr_transaction_values = merged_transactions_contracts.map(lambda entry: (entry[0], entry[1][0]))

    # Retrieve the top 10 smart contracts with the highest transaction values
    top10services = addr_transaction_values.takeOrdered(10, key=lambda item: -1 * item[1])

    # Set up S3 bucket resource for storing the results
    my_bucket_resource = boto3.resource('s3',
                                        endpoint_url='http://' + s3_endpoint_url,
                                        aws_access_key_id=s3_access_key_id,
                                        aws_secret_access_key=s3_secret_access_key)

    # Get the current date and time for the output filename
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H_%M_%S")
    
    # Construct the output file name with the date and time
    output_file_name = f"top10services_{date_time}.txt"

    # Save the top 10 smart contracts to the S3 bucket
    my_result_object = my_bucket_resource.Object(s3_bucket, output_file_name)
    my_result_object.put(Body=json.dumps(top10services))

    # Stop the Spark session
    spark.stop()
