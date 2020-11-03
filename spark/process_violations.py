import boto3
import json
import os
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import pyspark.sql.udf
import re
from sodapy import Socrata


def create_spark_session():
    """
    Instantiates a Spark session to allow for our processing of the NYC Parking Violations dataset
    :return: SparkSession object with which we can perform our tasks
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.10.0") \
        .getOrCreate()
    return spark


def get_socrata_client(socrata_domain, secret_name, region_name, secret_key):
    """
    Initializes an AWS Secrets Manager session from which we can retrieve the Socrata application key and query the
    NYC Parking Violations dataset.
    :param socrata_domain: The URL endpoint of the Socrata service where the target API is located
    :param secret_name: The AWS Secrets Manager secret name where the application key is stored
    :param region_name: The AWS region name where the Secrets Manager instance is located
    :param secret_key: The Secret key whose value is the application key needed to call Socrata
    :return: the Socrata client
    """

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])

    api_token = get_secret_value_response[secret_key]

    client = Socrata(domain=socrata_domain, app_token=api_token)
    return client


@f.udf(returnType=StringType())
def color_udf(r):
    """
    A Spark User-Defined-Function (udf) intended to process every row of the DataFrame's 'vehicle_color' column
    and match the most likely standard color code based on the officer's original input
    :param r: a single row of the DataFrame to be processed
    :return: a color code from a standardized list based on the outcome of the Regex match algorithm
    """

    color_list = ['B{}K', 'W{}H', 'G{}Y', 'B{}L', 'B{}R', 'G{}L', 'M{}R', 'O{}R', 'P{}K', 'P{}R', 'R{}D', 'T{}N',
                  'Y{}W']
    if r is not None:
        mid_pattern = "+[A-Z]{0,}"
        color_input = r.upper()

        for color in color_list:
            color_match = re.match(color.format(mid_pattern), color_input)
            if color_match is not None:
                # returns the first match. Codes above are two letters only, so we combine the first and last letter.
                return color[0] + color[-1]

        return 'OTH'

    else:
        return 'OTH'


def process_parking_violations(spark, dataset_id, output_data):
    """
    The main driver function which uses the Socrata client to retrieve the target dataset in batches and processes
    the data to standardize the 'vehicle_color' input and partition the data on year then month before writing the
    resulting JSON to HDFS where we can process it further in our cloud data warehouse.
    :param spark: the current SparkSession
    :param dataset_id: the alphanumeric ID of the source dataset from data.cityofnewyork.us
    :param output_data: The HDFS directory in which we'll write the processed JSON output
    :return: None
    """

    client = get_socrata_client("data.cityofnewyork.us", "udacity/deng", "us-east-1", "socrata_app_token")

    # Set the schema explicitly here first. Most important is that summons_number is a LongType since that column is
    # always populated in the dataset and provides an obvious choice for sorting and batching.
    schema = StructType([
        StructField("summons_number", LongType(), True),
        StructField("plate_id", StringType(), True),
        StructField("registration_state", StringType(), True),
        StructField("plate_type", StringType(), True),
        StructField("issue_date", StringType(), True),
        StructField("violation_code", StringType(), True),
        StructField("vehicle_body_type", StringType(), True),
        StructField("vehicle_make", StringType(), True),
        StructField("issuing_agency", StringType(), True),
        StructField("street_code1", StringType(), True),
        StructField("street_code2", StringType(), True),
        StructField("street_code3", StringType(), True),
        StructField("vehicle_expiration_date", StringType(), True),
        StructField("violation_location", StringType(), True),
        StructField("violation_precinct", StringType(), True),
        StructField("issuer_precinct", StringType(), True),
        StructField("issuer_code", StringType(), True),
        StructField("issuer_command", StringType(), True),
        StructField("issuer_squad", StringType(), True),
        StructField("violation_time", StringType(), True),
        StructField("violation_county", StringType(), True),
        StructField("violation_in_front_of_or_opposite", StringType(), True),
        StructField("house_number", StringType(), True),
        StructField("street_name", StringType(), True),
        StructField("date_first_observed", StringType(), True),
        StructField("law_section", StringType(), True),
        StructField("sub_division", StringType(), True),
        StructField("days_parking_in_effect", StringType(), True),
        StructField("from_hours_in_effect", StringType(), True),
        StructField("to_hours_in_effect", StringType(), True),
        StructField("vehicle_color", StringType(), True),
        StructField("unregistered_vehicle", StringType(), True),
        StructField("vehicle_year", StringType(), True),
        StructField("meter_number", StringType(), True),
        StructField("feet_from_curb", StringType(), True),
        StructField("intersecting_street", StringType(), True),
        StructField("time_first_observed", StringType(), True),
        StructField("violation_legal_code", StringType(), True),
        StructField("violation_description", StringType(), True),
        StructField("violation_post_code", StringType(), True)])

    where_clause_format = "summons_number > {}"
    starting_summons_number = 0
    rows_returned = 1

    while rows_returned > 0:
        # first, write the dataset to Pandas DataFrame because this is a straightforward format from which to convert to
        # SparkSQL DataFrame. The dataset is returned from Socrata as a JSON array.
        pdf = pd.DataFrame(data=client.get(dataset_id, where=where_clause_format.format(str(starting_summons_number)),
                                           order="summons_number", limit=500000),
                           columns=['summons_number', 'plate_id', 'registration_state', 'plate_type', 'issue_date',
                                    'violation_code', 'vehicle_body_type', 'vehicle_make', 'issuing_agency',
                                    'street_code1', 'street_code2', 'street_code3', 'vehicle_expiration_date',
                                    'violation_location', 'violation_precinct', 'issuer_precinct', 'issuer_code',
                                    'issuer_command', 'issuer_squad', 'violation_time', 'violation_county',
                                    'violation_in_front_of_or_opposite', 'house_number', 'street_name',
                                    'date_first_observed', 'law_section', 'sub_division', 'days_parking_in_effect',
                                    'from_hours_in_effect', 'to_hours_in_effect', 'vehicle_color',
                                    'unregistered_vehicle', 'vehicle_year', 'meter_number', 'feet_from_curb',
                                    'intersecting_street', 'time_first_observed', 'violation_legal_code',
                                    'violation_description', 'violation_post_code'])

        rows_returned = len(pdf.index)

        print('Processing the next batch: ' + str(rows_returned) + ' rows.')

        if rows_returned > 0:
            # Converting to numeric allows for more intuitive sorting later. String sorting on what are actually numeric
            # values can be unpredictable.
            pdf['summons_number'] = pd.to_numeric(pdf['summons_number'])

            sdf = spark.createDataFrame(pdf, schema)

            starting_summons_number = sdf.agg(f.max('summons_number')).collect()[0]["max(summons_number)"]

            # Add the standardized color code column and separate year and month numbers for partitioning later
            sdf_final = sdf.withColumn(
                "vehicle_color_standardized",
                color_udf("vehicle_color")).withColumn(
                "year_number",
                f.year("issue_date")).withColumn(
                "month_number",
                f.month("issue_date"))

            sdf_final.write.partitionBy('year_number', 'month_number').json(output_data, 'append')
            print('Wrote ' + str(rows_returned) + ' rows to ' + output_data)


def main():
    spark = create_spark_session()
    dataset_id = "pvqr-7yc4"  # Source: https://dev.socrata.com/foundry/data.cityofnewyork.us/pvqr-7yc4
    output_data = "hdfs:///parking_violations"

    process_parking_violations(spark, dataset_id, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
