from __future__ import annotations
from dataclasses import dataclass
import zipfile
from tqdm import tqdm
from typing import List, Tuple

import pendulum
import logging
import pandas as pd

from data_analysis.gtfs_fetcher import GTFSFetcher
from scrape_data.scrape_schedule_versions import create_schedule_list

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

BASE_URL = "https://transitfeeds.com"

# Last historical schedule available on transitfeeds.com
LAST_TRANSITFEEDS = pendulum.date(2023, 12, 7)
FIRST_CTA = pendulum.date(2023, 12, 16)


@dataclass
class GTFSFeed:
    """Class for storing GTFSFeed data.
    """
    stops: pd.DataFrame
    stop_times: pd.DataFrame
    routes: pd.DataFrame
    trips: pd.DataFrame
    calendar: pd.DataFrame
    calendar_dates: pd.DataFrame
    shapes: pd.DataFrame

    @classmethod
    def extract_data(cls, gtfs_zipfile: zipfile.ZipFile, version_id: str) -> GTFSFeed:
        """Load each text file in zipfile into a DataFrame

        Args:
            gtfs_zipfile (zipfile.ZipFile): Zipfile downloaded from
                transitfeeds.com or transitchicago.com e.g.
                https://transitfeeds.com/p/chicago-transit-authority/
                165/20220718/download or https://www.transitchicago.com/downloads/sch_data/
            version_id (str, optional): The schedule version in use.
                Defaults to None.

        Returns:
            GTFSFeed: A GTFSFeed object containing multiple DataFrames
                accessible by name.
        """
        data_dict = {}
        pbar = tqdm(cls.__annotations__.keys())
        for txt_file in pbar:
            pbar.set_description(f'Loading {txt_file}.txt')
            try:
                with gtfs_zipfile.open(f'{txt_file}.txt') as file:
                    df = pd.read_csv(file, dtype="object")
                    logger.info(f'{txt_file}.txt loaded')

            except KeyError as ke:
                logger.info(f"{gtfs_zipfile} is missing required file")
                logger.info(ke)
                df = None
            data_dict[txt_file] = df
        return cls(**data_dict)


@dataclass
class ScheduleFeedInfo:
    """Represents a single schedule version with feed start and end dates.
    """
    schedule_version: str
    feed_start_date: str
    feed_end_date: str
    transitfeeds: bool = True

    def __str__(self):
        if self.transitfeeds:
            label = ''
        else:
            label = '_cta'
        return f'v_{self.schedule_version}_fs_{self.feed_start_date}_fe_{self.feed_end_date}{label}'

    def __getitem__(self, item):
        if item not in frozenset(['schedule_version', 'feed_start_date', 'feed_end_date']):
            raise KeyError(item)
        return self.__dict__[item]

    @classmethod
    def from_pendulum(cls, version, start_date, end_date):
        return cls(version.format("YYYYMMDD"),
                   start_date.format("YYYY-MM-DD"),
                   end_date.format("YYYY-MM-DD"))

    @classmethod
    def from_dict(cls, d):
        return cls(d['schedule_version'],
                   d['feed_start_date'],
                   d['feed_end_date'])

    def interval(self):
        start = pendulum.parse(self.feed_start_date)
        end = pendulum.parse(self.feed_end_date)
        return pendulum.interval(start, end)

    def contains(self, date_str: str) -> bool:
        d = pendulum.parse(date_str)
        return d in self.interval()


class ScheduleIndexer:
    def __init__(self, cache_manager: CacheManager, month: int, year: int, start2022: bool = True):
        self.month = month
        self.year = year
        self.start2022 = start2022
        self.gtfs_fetcher = GTFSFetcher(cache_manager)
        self.schedules: List[ScheduleFeedInfo] = []
        self.get_transitfeeds_schedules()
        self.get_gtfs_schedules()

    @staticmethod
    def calculate_latest_rt_data_date() -> str:
        """Calculate the latest available date of real-time bus data

        Returns:
            str: A string of the latest date in YYYY-MM-DD format.
        """
        if pendulum.now("America/Chicago").hour >= 11:
            end_date = (
                pendulum.yesterday("America/Chicago")
                .date().format('YYYY-MM-DD')
            )
        else:
            end_date = (
                pendulum.now("America/Chicago").subtract(days=2)
                .date().format('YYYY-MM-DD')
            )
        return end_date

    def get_transitfeeds_schedules(self):
        transitfeeds_schedules = create_schedule_list(month=5, year=2022, start2022=True)
        for schedule_dict in transitfeeds_schedules:
            self.schedules.append(ScheduleFeedInfo.from_dict(schedule_dict))

    def get_gtfs_schedules(self):
        pd = lambda version: pendulum.parse(version).date()
        gtfs_versions = [version for version in self.gtfs_fetcher.get_versions() if pd(version) >= FIRST_CTA]
        print(gtfs_versions)
        gtfs_versions.append(self.calculate_latest_rt_data_date())
        current = gtfs_versions.pop(0)
        while gtfs_versions:
            next = gtfs_versions[0]
            sfi = ScheduleFeedInfo.from_pendulum(current, pd(current), pd(next).subtract(days=1))
            sfi.transitfeeds = False
            self.schedules.append(sfi)
            current = gtfs_versions.pop(0)

    def get_schedules(self):
        return self.schedules
