"""
Script to upload the data set of New York Taxi trips in S3 data lake
Sources
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
https://github.com/toddwschneider/nyc-taxi-data/blob/master/setup_files/raw_data_urls.txt

Example
>>> sample_url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2015-05.csv'
>>> destination_key = sample_url.split('/')[-1]
>>> save_to_s3(source_url=sample_url, s3_key=destination_key, s3_bucket='ef-sample-data')

"""
import argparse
import sys

import boto3
from dateutil import parser, relativedelta

import requests

s3 = boto3.resource('s3')

BASE_URL = 'https://s3.amazonaws.com/nyc-tlc/trip+data'


def save_to_s3(*, source_url, s3_key, s3_bucket):
    destination_bucket_resource = s3.Bucket(s3_bucket)
    global transfer_size_bytes
    transfer_size_bytes = 0
    file_size_bytes = int(requests.head(source_url).headers.get('Content-Length'))
    file_size_megabytes = round(file_size_bytes / (1024 * 1024), 2)

    def _print_transferred_bytes(bytes_transferred):
        global transfer_size_bytes
        transfer_size_bytes += bytes_transferred
        transfer_percentage = round((transfer_size_bytes/file_size_bytes) * 100, 2)
        sys.stdout.write('\033[F')  # back to previous line
        sys.stdout.write('\033[K')  # clear line
        print(f'Transferred {transfer_size_bytes} out of {file_size_bytes} bytes -  {transfer_percentage}%')

    def _upload_to_s3():

        destination_obj = destination_bucket_resource.Object(s3_key)
        with requests.get(source_url, stream=True) as response:
            destination_obj.upload_fileobj(response.raw, Callback=_print_transferred_bytes)
            transfer_size_megabytes = round(transfer_size_bytes / (1024 * 1024), 2)
            print(f's3://{s3_bucket}/{s3_key} imported successfully, total size: {transfer_size_bytes} bytes, {transfer_size_megabytes} MB')

    print(f'File size {source_url} is {file_size_bytes} bytes, {file_size_megabytes} MB')

    # this string is going to be overridden
    print(f'Starting the upload to s3://{s3_bucket}/{s3_key}')

    _upload_to_s3()


def generate_months(start_month_str: str, end_month_str: str):
    start_month = parser.parse(f'{start_month_str}-01')
    end_month = parser.parse(f'{end_month_str}-01')

    if start_month >= end_month:
        raise Exception('Insert a start month bigger than the end month')

    diff = relativedelta.relativedelta(start_month, end_month)

    month_diff = abs(diff.months) + 1

    for month in range(month_diff):
        yield (start_month + relativedelta.relativedelta(months=month)).strftime('%Y-%m')


def get_hive_partition_key(source, data_set_type, year, month, s3_key):
    return f'source={source}/data_set_type={data_set_type}/year={year}/month={month}/{s3_key}'


def get_data_set(data_set_types, start_month, end_month, destination_bucket_name):
    for data_set in data_set_types:
        for year_month in list(generate_months(start_month, end_month)):
            filename = f'{data_set}_{year_month}.csv'
            url = f'{BASE_URL}/{filename}'
            year = year_month.split('-')[0]
            month = year_month.split('-')[1]
            hive_partition_key = get_hive_partition_key('new_york_taxi', data_set, year, month, filename)

            save_to_s3(source_url=url, s3_bucket=destination_bucket_name, s3_key=hive_partition_key)


def create_argparser():
    arg_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    arg_parser.add_argument('--start-month', required=True, help='Start month')
    arg_parser.add_argument('--end-month', required=True, help='End month, included')
    arg_parser.add_argument('--destination-bucket', required=True, help='AWS S3 destination bucket name')

    return arg_parser


if __name__ == '__main__':
    args = create_argparser().parse_args()
    start_month_input = vars(args).get('start_month')
    end_month_input = vars(args).get('end_month')
    destination_bucket = vars(args).get('destination_bucket')

    data_sets = ['fhv_tripdata', 'green_tripdata', 'yellow_tripdata']

    get_data_set(data_sets, start_month_input, end_month_input, destination_bucket)
