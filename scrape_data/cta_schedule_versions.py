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

today = pendulum.now().to_date_string()

CTA_GTFS, zipfile_bytes_io = sga.download_cta_zip()

def save_cta_zip():
    print(f'Saving zipfile available at '
        f'https://www.transitchicago.com/downloads/sch_data/google_transit.zip '
        f'on {today} to public bucket')
    zipfile_bytes_io.seek(0)
    client.upload_fileobj(
        zipfile_bytes_io,
        'chn-ghost-buses-public',
        f'cta_schedule_zipfiles_raw/google_transit_{today}.zip'
    )
    print('Confirm that object exists in bucket')
    keys('chn-ghost-buses-public', [
                f'cta_schedule_zipfiles_raw/google_transit_{today}.zip'
            ])

def save_route_daily_summary():
    data = sga.GTFSFeed.extract_data(CTA_GTFS)
    data = sga.format_dates_hours(data)
    trip_summary = sga.make_trip_summary(data)

    route_daily_summary = (
        sga.summarize_date_rt(trip_summary)
    )
    route_daily_summary['date'] = route_daily_summary['date'].astype(str)
    route_daily_summary_today = route_daily_summary.loc[route_daily_summary['date'] == today]

    csv_buffer = StringIO()
    route_daily_summary_today.to_csv(csv_buffer)

    print(f'Saving cta_route_daily_summary_{today}.csv to public bucket')
    s3.Object(
        'chn-ghost-buses-public',
        f'schedule_summaries/daily_job/cta_route_daily_summary_{today}.csv')\
        .put(Body=csv_buffer.getvalue())

    print('Confirm that object exists in bucket')
    keys('chn-ghost-buses-public', [
                f'schedule_summaries/daily_job/cta_route_daily_summary_{today}.csv',
            ])


# https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3
def keys(bucket_name: str, filenames: list, prefix: str='/', delimiter: str='/', start_after: str=''):
    s3_paginator = client.get_paginator('list_objects_v2')
    prefix = prefix.lstrip(delimiter)
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            if content['Key'] in filenames:
                print(f"{content['Key']} exists")
