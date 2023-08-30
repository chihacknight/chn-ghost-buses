import boto3
import sys
import data_analysis.static_gtfs_analysis as sga
import data_analysis.compare_scheduled_and_rt as csrt
import pendulum
from io import StringIO, BytesIO
import pandas as pd
from typing import List

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


def save_sched_daily_summary(date_range: List[str, str] = None) -> None:
    if date_range is None:
        date_range = [today]
        print(f"No date range given. Using {today} only")
            
    start_date = pendulum.parse(min(date_range))
    end_date = pendulum.parse(max(date_range))
    period = pendulum.period(start_date, end_date)
    full_date_range = [dt.to_date_string() for dt in period.range('days')]
    zip_filename_list = [f'cta_schedule_zipfiles_raw/google_transit_{date}.zip'
                         for date in full_date_range]

    # Check for files in bucket.
    found_list = keys(
        csrt.BUCKET_PUBLIC,
        zip_filename_list
    )
    
    def extract_date(fname: str) -> str:
        return fname.split('_')[-1].split('.')[0]

    def create_route_summary(CTA_GTFS: sga.GTFSFeed) -> pd.DataFrame:
        data = sga.GTFSFeed.extract_data(CTA_GTFS)
        data = sga.format_dates_hours(data)
        trip_summary = sga.make_trip_summary(data)

        route_daily_summary = (
            sga.summarize_date_rt(trip_summary)
        )

        route_daily_summary['date'] = route_daily_summary['date'].astype(str)
        route_daily_summary_today = route_daily_summary.loc[route_daily_summary['date'].isin(date_range)]
        return route_daily_summary_today
    
    print('Using zipfiles found in public bucket')
    s3zip_list = []
    for fname in found_list:
        zip_bytes = BytesIO()
        zip_bytes.seek(0)
        client.download_fileobj(fname, zip_bytes)
        zipfilesched = sga.zipfile.Zipfile(zip_bytes)
        fdate = extract_date(fname)
        s3zip_list.append(
            {
                'zip_filename': fname, 
                'zip': zipfilesched,
                'csv_filename': f'schedule_summaries/daily_job/'
                                f'cta/cta_route_daily_summary_{fdate}.csv'
            }
        )    
    
    s3_route_daily_summary_dict = {
        'zip_filenames': [gtfs['zip_filename'] for gtfs in s3zip_list],
        'summaries': [create_route_summary(gtfs['zip']) for gtfs in s3zip_list],
        'csv_filenames': [gtfs['csv_filename'] for gtfs in s3zip_list]
    } 
    
    transitfeeds_list = list(set(zip_filename_list).difference(set(found_list)))
    print(', '.join(transitfeeds_list) + ' were not found in s3. Using transitfeeds.com')
    transitfeeds_dates = []  
    for fname in transitfeeds_list:
        # Extract date from string after splitting on '_' and then '.'
        fdate = extract_date(fname)
        transitfeeds_dates.append(fdate)
    
    
    transitfeeds_dates = sorted(transitfeeds_dates)
    schedule_list = csrt.create_schedule_list(month=5, year=2022)
    schedule_list_filtered = [
        s for s in schedule_list 
        if s['feed_start_date'] >= min(transitfeeds_dates)
        and s['feed_start_date'] <= max(transitfeeds_dates)
    ]
    

    trip_summaries_transitfeeds_dict = {'zip_filenames': [], 'zips': [], 'csv_filenames': [],
                                        'summaries': []}
    
    for sched in schedule_list_filtered:
        CTA_GTFS, zipfile_bytes_io = sga.download_zip(sched['schedule_version'])
        trip_summaries_transitfeeds_dict['zip_filenames'].append(
            f"transitfeeds_schedule_zipfiles_raw/{sched['schedule_version']}.zip"
        )
        trip_summaries_transitfeeds_dict['zips'].append((CTA_GTFS, zipfile_bytes_io))
        trip_summaries_transitfeeds_dict['summaries'].append(create_route_summary(CTA_GTFS))
        trip_summaries_transitfeeds_dict['csv_filenames'].append(
            f'schedule_summaries/daily_job/transitfeeds/'
            f'transitfeeds_route_daily_summary_v{sched["schedule_version"]}.csv'
        )

    print(f'Saving cta schedule summary files in {date_range} to public bucket')
    for filename, summary in zip(
        s3_route_daily_summary_dict['csv_filenames'],
        s3_route_daily_summary_dict['summaries']
    ):
        save_csv_to_bucket(summary, filename=filename)

    print(f'Saving transitfeeds schedule summary files and zip files '
          f'in {date_range} to public bucket')
    for csv_filename, summary, zip_filename, zipfile in zip(
        trip_summaries_transitfeeds_dict['csv_filenames'],
        trip_summaries_transitfeeds_dict['summaries'],
        trip_summaries_transitfeeds_dict['zip_filenames'],
        trip_summaries_transitfeeds_dict['zips']
    ):
        save_csv_to_bucket(summary, filename=csv_filename)
        # Save the zip file
        client.upload_fileobj(
            zipfile[1],
            csrt.BUCKET_PUBLIC,
            zip_filename
        )

    for fname in ['csv_filenames', 'zip_filenames']:
        print('Confirm that ' + ', '.join(s3_route_daily_summary_dict[fname])
            + ' exist in bucket')  
        _ = keys(csrt.BUCKET_PUBLIC, s3_route_daily_summary_dict[fname])
        
        print('Confirm that ' + ', '.join(trip_summaries_transitfeeds_dict[fname])
            + ' exists in bucket')
        _ = keys(csrt.BUCKET_PUBLIC, trip_summaries_transitfeeds_dict[fname])

def save_realtime_daily_summary(date: str = None) -> None:
    if date is None:
        if pendulum.now("America/Chicago").hour >= 11:
            date = pendulum.yesterday("America/Chicago")
        else: 
            date = pendulum.now("America/Chicago").subtract(days=2)

        date = date.to_date_string()
        print(f'Date not given. Taking the latest available date {date}.')
    else:
        date = pendulum.parse(date).strftime('%Y-%m-%d')

    daily_data = pd.read_csv(
                (csrt.BASE_PATH / f"bus_full_day_data_v2/{date}.csv")
                .as_uri(),
                low_memory=False
            )

    daily_data = csrt.make_daily_summary(daily_data)
    filename = f'realtime_summaries/daily_job/bus_full_day_data_v2/{date}.csv'
    save_csv_to_bucket(daily_data, filename=filename)

    print(f'Confirm that {filename} exists in bucket')
    _ = keys(csrt.BUCKET_PUBLIC, [filename])

# https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3
def keys(bucket_name: str, filenames: list,
         prefix: str='/', delimiter: str='/',
         start_after: str='') -> list:
    s3_paginator = client.get_paginator('list_objects_v2')
    prefix = prefix.lstrip(delimiter)
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    found_list = []
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            if content['Key'] in filenames:
                print(f"{content['Key']} exists")
                found_list.append(content['Key'])
    return found_list