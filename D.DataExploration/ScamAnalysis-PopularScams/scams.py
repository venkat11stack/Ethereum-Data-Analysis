import sys, string
import os
import socket
import time
import operator
import boto3
import json
from io import StringIO
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    # It processes the JSON data to generate a list of comma-separated strings (CSV format).
    def process_json_to_csv_dict(json_dict):
        
        # Initialize an empty list to store the processed CSV data.
        csv_data = []

        # Iterate through the keys (addresses) in the 'result' field of the JSON data.
        for address in json_dict['result'].keys():
            # Iterate through the list of addresses in the 'addresses' field of each address entry.
            for i in json_dict['result'][address]['addresses']:
                # Extract relevant data fields and convert them to strings.
                identity = str(json_dict['result'][address]['id'])
                name = str(json_dict['result'][address]['name'])
                url = str(json_dict['result'][address]['url'])
                coin = str(json_dict['result'][address]['coin'])
                # If the category is not 'Scam', use the original category value.
                # Otherwise, replace it with 'Scamming'.
                if json_dict['result'][address]['category'] != 'Scam':
                    category = str(json_dict['result'][address]['category'])
                else:
                    category = 'Scamming'

                # Check if the 'subcategory' field exists in the address entry.
                # If it exists, convert it to a string. Otherwise, use an empty string.
                if 'subcategory' in json_dict['result'][address]:
                    subcategory = str(json_dict['result'][address]['subcategory'])
                else:
                    subcategory = ""

                # Convert the index and status fields to strings.
                index = str(i)
                status = str(json_dict['result'][address]['status'])

                # Create a CSV string by joining the extracted fields using commas.
                csv = identity + ',' + name + ',' + url + ',' + coin + ',' + category + ',' + subcategory + ',' + index + ',' + status

                # Append the created CSV string to the csv_data list.
                csv_data.append(csv)

        return csv_data

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

    # Read transactions from S3 bucket
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")

    # Read scams JSON data from the S3 bucket
    scams_json_rdd = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.json")

    # Convert the JSON data to a dictionary
    scams_dict = json.loads(scams_json_rdd.collect()[0])

    # Process the JSON data to CSV format using the modified function
    scams_csv_data = process_json_to_csv_dict(scams_dict)

    # Create an RDD from the processed CSV data
    scams = spark.sparkContext.parallelize(scams_csv_data)

    # Filter out invalid transactions and scams
    transactions = transactions.filter(lambda l: len(l.split(',')) == 15 and l.split(',')[7].replace('.', '', 1).isdigit())
    scams = scams.filter(lambda l: len(l.split(',')) == 8 and l.split(',')[0].isdigit())

    # Map scams to their address and ID/category tuples
    scams_mapped = scams.map(lambda l: (l.split(',')[6], (l.split(',')[0], l.split(',')[4])))

    # Map transactions to their address and Ether received
    transactions_mapped = transactions.map(lambda l: (l.split(',')[6], float(l.split(',')[7])))

    # Join transactions and scams on the address
    joined_data = transactions_mapped.join(scams_mapped)

    # Map joined data to the scam ID/category and Ether received
    scams_with_received_ether = joined_data.map(lambda x: ((x[1][1][0], x[1][1][1]), x[1][0]))

    # Calculate the total Ether received for each scam ID/category
    scams_total_ether = scams_with_received_ether.reduceByKey(lambda a, b: a + b)

    # Get the top 15 most lucrative scams
    top15_scams = scams_total_ether.takeOrdered(15, key=lambda x: -x[1])
    print(top15_scams)

    # Map transactions to their address and month/year with Ether received
    transactions_month_year = transactions.map(lambda l: (l.split(',')[6], (time.strftime("%m/%Y", time.gmtime(int(l.split(',')[11]))), float(l.split(',')[7]))))

    # Join transactions with scams on the address
    joined_data_month_year = transactions_month_year.join(scams_mapped)

      # Map joined data to the month/year and scam category with Ether received
    ether_by_time_and_category = joined_data_month_year.map(lambda x: ((x[1][0][0], x[1][1]), x[1][0][1]))

    # Calculate the total Ether received for each scam category in each month/year
    ether_by_time_and_category_total = ether_by_time_and_category.reduceByKey(lambda a, b: a + b)
    ether_by_time_and_category_total = ether_by_time_and_category_total.map(lambda a: ((a[0][0], a[0][1]), float(a[1])))

    # Set up the S3 bucket resource for uploading results
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    # Get the current date and time
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H_%M_%S")
    
    # Construct the output file name with the date and time
    output_file_name1 = f"popular_scams_{date_time}.txt"
    output_file_name2 = f"ether_against_time_{date_time}.txt"
    
    # Upload the top 15 most lucrative scams to the S3 bucket
    my_result_object = my_bucket_resource.Object(s3_bucket, output_file_name1)
    my_result_object.put(Body=json.dumps(top15_scams))

    # Upload the Ether received by scam category and month/year to the S3 bucket
    my_result_object = my_bucket_resource.Object(s3_bucket, output_file_name2)
    my_result_object.put(Body=json.dumps(ether_by_time_and_category_total.collect()))

    # Stop the Spark session
    spark.stop()