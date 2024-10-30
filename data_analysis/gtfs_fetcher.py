from __future__ import annotations

import os
from enum import Enum
from dataclasses import dataclass

# required for pandas to read csv from aws
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pendulum

from data_analysis.cache_manager import CacheManager

BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')

# Enable reading from public buckets without setting up credentials
# https://stackoverflow.com/questions/34865927/can-i-use-boto3-anonymously
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))


class ScheduleSource(Enum):
    TRANSITFEEDS = 1
    S3 = 2


@dataclass
class ScheduleFeedInfo:
    """Represents a single schedule version with feed start and end dates.
    """
    schedule_version: str
    feed_start_date: str
    feed_end_date: str
    schedule_source: ScheduleSource

    def __str__(self):
        if self.schedule_source == ScheduleSource.TRANSITFEEDS:
            label = ''
        else:
            label = '_cta'
        return f'v_{self.schedule_version}_fs_{self.feed_start_date}_fe_{self.feed_end_date}{label}'

    def __getitem__(self, item):
        if item not in frozenset(['schedule_version', 'feed_start_date', 'feed_end_date']):
            raise KeyError(item)
        return self.__dict__[item]

    @classmethod
    def from_pendulum(cls, version, start_date, end_date, schedule_source: ScheduleSource):
        return cls(version.format("YYYYMMDD"),
                   start_date.format("YYYY-MM-DD"),
                   end_date.format("YYYY-MM-DD"),
                   schedule_source)

    @classmethod
    def from_dict(cls, d):
        return cls(d['schedule_version'],
                   d['feed_start_date'],
                   d['feed_end_date'],
                   ScheduleSource.TRANSITFEEDS)

    def interval(self):
        start = pendulum.parse(self.feed_start_date)
        end = pendulum.parse(self.feed_end_date)
        return pendulum.interval(start, end)

    def contains(self, date_str: str) -> bool:
        d = pendulum.parse(date_str)
        return d in self.interval()


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

    def retrieve_file(self, schedule_feed_info: ScheduleFeedInfo):
        version_id = schedule_feed_info.schedule_version
        if schedule_feed_info.schedule_source == ScheduleSource.S3:
            local_filename, _, s3_filename, _ = self.versions[version_id]
            url = f'https://{BUCKET_PUBLIC}.s3.us-east-2.amazonaws.com/{s3_filename}'
            return self.cache_manager.retrieve('cta_zipfiles', local_filename, url)
        elif schedule_feed_info.schedule_source == ScheduleSource.TRANSITFEEDS:
            url = f"https://transitfeeds.com/p/chicago-transit-authority/165/{version_id}/download"
            return self.cache_manager.retrieve("downloads", f"{version_id}.zip", url)
        else:
            return None


# Prints cached downloaded schedules for local debugging purposes. Run from project root:
#  python -m data_analysis.gtfs_fetcher
if __name__ == "__main__":
    fetcher = GTFSFetcher(CacheManager())
    for filename, size, fullkey, version in fetcher.list():
        print(f'{version}  {filename:30}  {size:10} {fullkey}')
