# data-sets-s3-uploader
Upload sample data sets to S3

## Requirements
* Python >= 3
* Install requirements `pin install -r requirements.txt`
* AWS S3 Bucket
* AWS IAM User with write permission to the destination S3 bucket
* Local configuration to access AWS, follow this [link](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)


## Data Sets
* [New York Taxi Data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

## Usage

### Taxi Data
<pre>
python taxi_uploader.py --start-month 2018-01 --end-month 2015-04 --destination-bucket my-example-bucket
</pre>
