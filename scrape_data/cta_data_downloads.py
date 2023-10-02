import boto3
import sys
import data_analysis.static_gtfs_analysis as sga
import data_analysis.compare_scheduled_and_rt as csrt
import data_analysis.plots as plots
import pendulum
from io import StringIO, BytesIO
import pandas as pd
import typing

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


def save_transitfeeds_zip(start_date: str = '2022-05-20', end_date: str = today) -> None:
    schedule_list = csrt.create_schedule_list(month=5, year=2022)
    schedule_list_filtered = [
        s for s in schedule_list 
        if s['feed_start_date'] >= start_date
        and s['feed_start_date'] <= end_date
    ]
    filename_list = []
    for schedule_dict in schedule_list_filtered:
        version = schedule_dict['schedule_version']    
        _, zipfile_bytes_io = sga.download_zip(version_id=version)
        zip_filename = f"transitfeeds_schedule_zipfiles_raw/{version}.zip"
        filename_list.append(zip_filename)
        zipfile_bytes_io.seek(0)
        client.upload_fileobj(
                zipfile_bytes_io,
                csrt.BUCKET_PUBLIC,
                zip_filename
            )        
    print(f'Confirm that files exist in s3')
    keys('chn-ghost-buses-public', filename_list)
 


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


def find_s3_zipfiles(date_range: typing.List[str]) -> typing.Tuple[list]:
    start_date = pendulum.parse(min(date_range))
    end_date = pendulum.parse(max(date_range))
    period = pendulum.period(start_date, end_date)
    full_date_range = [dt.to_date_string() for dt in period.range('days')]
    zip_filename_list = [f'cta_schedule_zipfiles_raw/google_transit_{date}.zip'
                         for date in full_date_range]
    found_list = keys(
        csrt.BUCKET_PUBLIC,
        zip_filename_list
    )
    return zip_filename_list, found_list


def find_transitfeeds_zipfiles(
        full_list: typing.List[str],
        found_list: typing.List[str]) -> typing.List[str]:
    
    transitfeeds_list = list(set(full_list).difference(set(found_list)))
    if transitfeeds_list:
        print(', '.join(transitfeeds_list) + ' were not found in s3. Using transitfeeds.com')
        transitfeeds_dates = []  
        for fname in transitfeeds_list:
            # Extract date from string after splitting on '_' and then '.'
            fdate = extract_date(fname)
            transitfeeds_dates.append(fdate)
        
        
        transitfeeds_dates = sorted(transitfeeds_dates)
        schedule_list = csrt.create_schedule_list(month=5, year=2022)
        # Start of saving transitchicago.com zipfiles to s3 was 2023-07-28. Don't need to check
        # after this date.
        schedule_list_filtered = [
            s for s in schedule_list 
            if s['feed_start_date'] >= min(transitfeeds_dates)
            and s['feed_start_date'] <= '2023-07-28' 
        ]
        return schedule_list_filtered
    else:
        print("All records found in s3 from transitchicago.com")
        return full_list


def download_s3_file(fname: str) -> sga.GTFSFeed:
    print(f'Downloading {fname} from s3')
    zip_bytes = BytesIO()
    zip_bytes.seek(0)
    client.download_fileobj(Bucket=sga.BUCKET, Key=fname, Fileobj=zip_bytes)
    zipfilesched = sga.zipfile.ZipFile(zip_bytes)
    data = sga.GTFSFeed.extract_data(zipfilesched)
    data = sga.format_dates_hours(data)
    return data


def compare_realtime_sched(
        date_range: typing.List[str] = ['2022-05-20', today]) -> None:
           
    zip_filename_list, found_list = find_s3_zipfiles(date_range=date_range)
    schedule_list_filtered = find_transitfeeds_zipfiles(zip_filename_list, found_list)
    # Extract data from s3 zipfiles
    s3_data_list = []
    for fname in found_list:
        data = download_s3_file(fname)
        s3_data_list.append({'fname': fname, 'data': data})
    
    for tfdict in schedule_list_filtered:
        version = tfdict['schedule_version']
        full_name = f"transitfeeds_schedule_zipfiles_raw/{version}.zip"
        tfdata = download_s3_file(full_name)
        s3_data_list.append({'fname': version, 'data': tfdata})
        
    # Convert from list of dictionaries to dictionary with list values
    joined_dict = pd.DataFrame(s3_data_list).to_dict(orient='list')
    schedule_data_list = [{'schedule_version': fname, 'data': create_route_summary(data, date_range)}
      for fname, data in joined_dict.items()]

    agg_info = csrt.AggInfo()
    print('Creating combined_long_df and summary_df')
    combined_long_df, summary_df = csrt.combine_real_time_rt_comparison(
        schedule_feeds=schedule_list_filtered,
        schedule_data_list=schedule_data_list,
        agg_info=agg_info
    )    

    day_type = 'wk'
    start_date = combined_long_df["date"].min().strftime("%Y-%m-%d")
    end_date = combined_long_df["date"].max().strftime("%Y-%m-%d")

    summary_gdf_geo = plots.create_summary_gdf_geo(combined_long_df, summary_df, day_type=day_type)
    summary_kwargs = {'column': 'ratio'}
    save_name = f"all_routes_{start_date}_to_{end_date}_{day_type}"

    plots.save_json(
        summary_gdf_geo=summary_gdf_geo, 
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )
    
    s3_data_json_path = 'frontend_data_files/data.json'
    print(f'Saving data.json to {s3_data_json_path}')

    data_json = plots.create_frontend_json(
        json_file=f'{save_name}.json',
        start_date=start_date,
        end_date=end_date,
        save=False
    )
    # Save data.json to s3 for now. This will eventually live in the frontend repo.
    s3.Object(
        csrt.BUCKET_PUBLIC,
        f'{s3_data_json_path}')\
        .put(Body=data_json)

    _ = keys(csrt.BUCKET_PUBLIC, ['data.json'])


def confirm_saved_files(file_dict: dict) -> None:
    for fname in ['csv_filenames', 'zip_filenames']:
        print('Confirm that ' + ', '.join(file_dict[fname])
            + ' exist in bucket')  
        _ = keys(csrt.BUCKET_PUBLIC, file_dict[fname])


def extract_date(fname: str) -> str:
    return fname.split('_')[-1].split('.')[0]


def create_route_summary(CTA_GTFS: sga.GTFSFeed, date_range: typing.List[str]) -> pd.DataFrame:
    data = sga.GTFSFeed.extract_data(CTA_GTFS)
    data = sga.format_dates_hours(data)
    trip_summary = sga.make_trip_summary(data)

    route_daily_summary = (
        sga.summarize_date_rt(trip_summary)
    )

    route_daily_summary['date'] = route_daily_summary['date'].astype(str)
    route_daily_summary_today = route_daily_summary.loc[route_daily_summary['date'].isin(date_range)]
    return route_daily_summary_today


def save_sched_daily_summary(date_range: typing.List[str] = None) -> None:
    if date_range is None:
        date_range = [today]
        print(f"No date range given. Using {today} only")
            
    zip_filename_list, found_list = find_s3_zipfiles(date_range=date_range)
 
    print('Using zipfiles found in public bucket')
    s3zip_list = []
    for fname in found_list:
        zip_bytes = BytesIO()
        zip_bytes.seek(0)
        client.download_fileobj(Bucket=sga.BUCKET, Key=fname, Fileobj=zip_bytes)
        zipfilesched = sga.zipfile.ZipFile(zip_bytes)
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
        'summaries': [create_route_summary(gtfs['zip'], date_range) for gtfs in s3zip_list],
        'csv_filenames': [gtfs['csv_filename'] for gtfs in s3zip_list]
    }

    print(f'Saving cta schedule summary files in {date_range} to public bucket')
    for filename, summary in zip(
        s3_route_daily_summary_dict['csv_filenames'],
        s3_route_daily_summary_dict['summaries']
    ):
        save_csv_to_bucket(summary, filename=filename)
    
    confirm_saved_files(s3_route_daily_summary_dict)
    schedule_list_filtered = find_transitfeeds_zipfiles(zip_filename_list, found_list)
    
    # Only download transitfeeds for files not found in s3.
    if set(found_list) != set(zip_filename_list):
        trip_summaries_transitfeeds_dict = {'zip_filenames': [], 'zips': [], 'csv_filenames': [],
                                            'summaries': []}
        
        for sched in schedule_list_filtered:
            CTA_GTFS, zipfile_bytes_io = sga.download_zip(sched['schedule_version'])
            trip_summaries_transitfeeds_dict['zip_filenames'].append(
                f"transitfeeds_schedule_zipfiles_raw/{sched['schedule_version']}.zip"
            )
            trip_summaries_transitfeeds_dict['zips'].append((CTA_GTFS, zipfile_bytes_io))
            trip_summaries_transitfeeds_dict['summaries'].append(create_route_summary(CTA_GTFS, date_range))
            trip_summaries_transitfeeds_dict['csv_filenames'].append(
                f'schedule_summaries/daily_job/transitfeeds/'
                f'transitfeeds_route_daily_summary_v{sched["schedule_version"]}.csv'
            )
        print(
            f'Saving transitfeeds schedule summary files and zip files '
            f'in {date_range} to public bucket'
        )
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
        confirm_saved_files(trip_summaries_transitfeeds_dict)
    

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