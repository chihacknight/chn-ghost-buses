from __future__ import annotations
from dataclasses import dataclass
import zipfile
from tqdm import tqdm
from typing import List, Tuple

import pendulum
import logging
import pandas as pd

from scrape_data.scrape_schedule_versions import create_schedule_list
from data_analysis.gtfs_fetcher import GTFSFetcher, ScheduleFeedInfo, ScheduleSource

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
        pbar.set_description(f'Loading schedule {version_id}')
        for txt_file in pbar:
            try:
                with gtfs_zipfile.open(f'{txt_file}.txt') as file:
                    df = pd.read_csv(file, dtype="object")

            except KeyError as ke:
                logger.info(f"{gtfs_zipfile} is missing required file")
                logger.info(ke)
                df = None
            data_dict[txt_file] = df
        return cls(**data_dict)


class ScheduleIndexer:
    def __init__(self, cache_manager: CacheManager, month: int, year: int):
        self.month = month
        self.year = year
        self.gtfs_fetcher = GTFSFetcher(cache_manager)
        self.schedules: List[ScheduleFeedInfo] = []
        schedule_start = pendulum.date(self.year, self.month, 1)
        if schedule_start <= LAST_TRANSITFEEDS:
            self.get_transitfeeds_schedules()
        else:
            logger.info(f'Skipping transitfeeds schedule fetch because schedule start {schedule_start} is after the '
                        'last available transitfeeds schedule.')
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
        transitfeeds_schedules = create_schedule_list(month=self.month,
                                                      year=self.year)
        for schedule_dict in transitfeeds_schedules:
            self.schedules.append(ScheduleFeedInfo.from_dict(schedule_dict))

    def get_gtfs_schedules(self):
        pd = lambda version: pendulum.parse(version).date()
        gtfs_versions = [version for version in self.gtfs_fetcher.get_versions() if pd(version) >= FIRST_CTA]
        gtfs_versions.append(self.calculate_latest_rt_data_date())
        current = gtfs_versions.pop(0)
        while gtfs_versions:
            next = gtfs_versions[0]
            sfi = ScheduleFeedInfo.from_pendulum(current,
                                                 pd(current).add(days=1),
                                                 pd(next).subtract(days=1),
                                                 ScheduleSource.S3)
            self.schedules.append(sfi)
            current = gtfs_versions.pop(0)

    def get_schedules(self):
        return self.schedules
