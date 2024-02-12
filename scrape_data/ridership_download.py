import boto3
import sys
import data_analysis.ridership_to_json as ridership_to_json
import data_analysis.static_gtfs_analysis as sga

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

def save_ridership_json() -> None:
    ridership_json = ridership_to_json.main(save=False)
    s3_ridership_json_path = 'frontend_data_files/cta_ridership_data_day_type_summary.json'
    print(f'Saving {s3_ridership_json_path}')
    s3.Object(
        sga.BUCKET_PUBLIC,
        f'{s3_ridership_json_path}')\
        .put(Body=ridership_json)

    # Check that the file was uploaded successfully
    keys(sga.BUCKET_PUBLIC, [s3_ridership_json_path])


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
                