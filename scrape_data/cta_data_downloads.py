import boto3
import sys
import data_analysis.static_gtfs_analysis as sga
import data_analysis.compare_scheduled_and_rt as csrt
import pendulum
from io import StringIO
import pandas as pd


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

today = pendulum.now('America/Chicago').to_date_string()

CTA_GTFS, zipfile_bytes_io = sga.download_cta_zip()

def save_cta_zip() -> None:
    print(f'Saving zipfile available at '
        f'https://www.transitchicago.com/downloads/sch_data/google_transit.zip '
        f'on {today} to public bucket')
    filename = f'cta_schedule_zipfiles_raw/google_transit_{today}.zip'
    zipfile_bytes_io.seek(0)
    client.upload_fileobj(
        zipfile_bytes_io,
        csrt.BUCKET_PUBLIC,
        filename
    )
    print(f'Confirm that {filename} exists in bucket')
    keys('chn-ghost-buses-public', [filename])


def save_csv_to_bucket(df: pd.DataFrame, filename: str) -> None:
    """Save pandas DataFrame to csv in s3

    Args:
        df (pd.DataFrame): DataFrame to be saved
        filename (str): Name of the saved filename in s3.
            Should contain the .csv suffix.
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    print(f'Saving {filename} to public bucket')
    s3.Object(
        csrt.BUCKET_PUBLIC,
        f'{filename}')\
        .put(Body=csv_buffer.getvalue())


def save_sched_daily_summary() -> None:
    data = sga.GTFSFeed.extract_data(CTA_GTFS)
    data = sga.format_dates_hours(data)
    trip_summary = sga.make_trip_summary(data)

    route_daily_summary = (
        sga.summarize_date_rt(trip_summary)
    )
    route_daily_summary['date'] = route_daily_summary['date'].astype(str)
    route_daily_summary_today = route_daily_summary.loc[route_daily_summary['date'] == today]

    print(f'Saving cta_route_daily_summary_{today}.csv to public bucket')
    filename = f'schedule_summaries/daily_job/cta_route_daily_summary_{today}.csv'
    save_csv_to_bucket(
        route_daily_summary_today,
        filename=filename
    )
    print(f'Confirm that {filename} exists in bucket')
    keys(csrt.BUCKET_PUBLIC, [filename])


def save_realtime_daily_summary() -> None:
    if pendulum.now("America/Chicago").hour >= 11:
        end_date = pendulum.yesterday("America/Chicago")
    else: 
        end_date = pendulum.now("America/Chicago").subtract(days=2)
    
    end_date = end_date.to_date_string()

    daily_data = pd.read_csv(
                (csrt.BASE_PATH / f"bus_full_day_data_v2/{end_date}.csv")
                .as_uri(),
                low_memory=False
            )

    daily_data = csrt.make_daily_summary(daily_data)
    filename = f'realtime_summaries/daily_job/bus_full_day_data_v2/{end_date}.csv'
    save_csv_to_bucket(daily_data, filename=filename)

    print(f'Confirm that {filename} exists in bucket')
    keys(csrt.BUCKET_PUBLIC, [filename])

# https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3
def keys(bucket_name: str, filenames: list,
         prefix: str='/', delimiter: str='/',
         start_after: str='') -> None:
    s3_paginator = client.get_paginator('list_objects_v2')
    prefix = prefix.lstrip(delimiter)
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            if content['Key'] in filenames:
                print(f"{content['Key']} exists")
