import boto3
import sys
import data_analysis.static_gtfs_analysis as sga
import pendulum
from io import StringIO

ACCESS_KEY = sys.argv[1]
SECRET_KEY = sys.argv[2]

client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

s3 = boto3.resource(
    's3',
    region_name='us-east-1',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

data = sga.download_extract_format()
trip_summary = sga.make_trip_summary(data)

route_daily_summary = (
    sga.summarize_date_rt(trip_summary)
)
date = pendulum.now().to_date_string()

csv_buffer = StringIO()
route_daily_summary.to_csv(csv_buffer)

s3.Object('chn-ghost-buses-public', f'cta_route_daily_summary_{date}.csv')\
    .put(Body=csv_buffer.getvalue())

