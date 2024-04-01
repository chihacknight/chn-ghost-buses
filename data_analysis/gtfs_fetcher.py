import os

# required for pandas to read csv from aws
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from data_analysis.cache_manager import CacheManager

BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')

# Enable reading from public buckets without setting up credentials
# https://stackoverflow.com/questions/34865927/can-i-use-boto3-anonymously
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))


class GTFSFetcher:
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        files = s3.list_objects_v2(Bucket=BUCKET_PUBLIC, Prefix='cta_schedule_zipfiles_raw/')
        self.unique_files = {}
        self.versions = {}
        for fc in files['Contents']:
            key = fc['ETag']
            filename = fc['Key'].split('/')[1]
            if not filename.startswith('google_transit_'):
                continue
            size = fc['Size']
            version = filename.removeprefix('google_transit_').removesuffix('.zip').replace('-', '')
            tup = (filename, size, fc['Key'], version)
            self.unique_files.setdefault(key, []).append((filename, size, fc['Key'], version))
        for v in self.unique_files.values():
            v.sort()
            tup = v[0]
            self.versions[tup[-1]] = tup

    def list(self):
        tups = []
        for v in self.unique_files.values():
            tups.append(v[0])
        tups.sort()
        return tups

    def get_versions(self):
        return list(sorted(self.versions.keys()))

    def retrieve_file(self, version):
        tup = self.versions[version]
        filename, size, s3_filename, _ = tup
        url = f'https://{BUCKET_PUBLIC}.s3.us-east-2.amazonaws.com/{s3_filename}'
        return self.cache_manager.retrieve('cta_zipfiles', filename, url)


if __name__ == "__main__":
    fetcher = GTFSFetcher(CacheManager())
    for filename, size, fullkey, version in fetcher.list():
        print(f'{version}  {filename:30}  {size:10} {fullkey}')
