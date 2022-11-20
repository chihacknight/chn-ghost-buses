# Goals:
#
# * Make a way to calculate the scheduled number of current active trips given
#  a date, time, and route.
#     - Take datetime and find what services are active on that date
#     - Find what trips run on those services + route
#     - Find which of those trips are "in progress" per stop_times
# * ~Output most common shape by route~


# imports
from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass
from typing import List

import logging
import calendar
import pandas as pd
import zipfile
import requests
import pendulum
from io import BytesIO
import shapely
import geopandas

from tqdm import tqdm
from scrape_data.scrape_schedule_versions import create_schedule_list

VERSION_ID = "20220718"
BUCKET = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)


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
    def extract_data(cls, gtfs_zipfile: zipfile.ZipFile,
                     version_id: str = None) -> GTFSFeed:
        """Load each text file in zipfile into a DataFrame

        Args:
            gtfs_zipfile (zipfile.ZipFile): Zipfile downloaded from
                CTA transit feeds e.g.
                https://transitfeeds.com/p/chicago-transit-authority/
                165/20220718/download"
            version_id (str, optional): The schedule version in use.
                Defaults to None.

        Returns:
            GTFSFeed: A GTFSFeed object containing multiple DataFrames
                accessible by name.
        """
        if version_id is None:
            version_id = VERSION_ID
        logging.info(f"Extracting data from CTA zipfile version {version_id}")
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


# Basic data transformations
# Ex. creating actual timestamps
def get_hour(s: str) -> int:
    """Return the hour from a string timestamp

    Args:
        s (str): A string of a timestamp e.g. "HH:MM:SS"

    Returns:
        int: the hour of the timestamp
    """
    parts = s.split(":")
    assert len(parts) == 3
    hour = int(parts[0])
    if hour >= 24:
        hour -= 24
    return hour


def format_dates_hours(data: GTFSFeed) -> GTFSFeed:
    """ Convert string dates to actual datetimes in calendar.txt and
        calendar_dates.txt
    Args:
        data (GTFSFeed): GTFS data from CTA

    Returns:
        GTFSFeed: GTFS data with datetimes and arrival and departue hours.
    """

    data.calendar["start_date_dt"] = data.calendar["start_date"].apply(
        lambda x: pendulum.from_format(x, "YYYYMMDD")
    )
    data.calendar["end_date_dt"] = data.calendar["end_date"].apply(
        lambda x: pendulum.from_format(x, "YYYYMMDD")
    )
    data.calendar_dates["date_dt"] = data.calendar_dates["date"].apply(
        lambda x: pendulum.from_format(x, "YYYYMMDD")
    )

    # extract hour from stop_times timestamps
    data.stop_times["arrival_hour"] = data.stop_times.arrival_time.apply(
        lambda x: get_hour(x)
    )
    data.stop_times["departure_hour"] = data.stop_times.departure_time.apply(
        lambda x: get_hour(x)
    )

    return data


def make_trip_summary(
    data: GTFSFeed,
    feed_start_date: pendulum.datetime,
        feed_end_date: pendulum.datetime) -> pd.DataFrame:
    """Create a summary of trips with one row per date

    Args:
        data (GTFSFeed): GTFS data from CTA
        feed_start_date (datetime): Date from which this feed is valid (inclusive)
        feed_end_date (datetime): Date until which this feed is valid (inclusive)

    Returns:
        pd.DataFrame: A DataFrame with each trip that occurred per row.
    """
    # construct a datetime index that has every day between calendar start and
    # end
    calendar_date_range = pd.DataFrame(
        pd.date_range(
            min(data.calendar.start_date_dt),
            max(data.calendar.end_date_dt)
        ),
        columns=["raw_date"],
    )

    # cross join calendar index with actual calendar to get all combos of
    # possible dates & services
    calendar_cross = calendar_date_range.merge(data.calendar, how="cross")

    # extract day of week from date index date
    calendar_cross["dayofweek"] = calendar_cross["raw_date"].dt.dayofweek

    # take wide calendar data (one col per day of week) and make it long (one
    # row per day of week)
    actual_service = calendar_cross.melt(
        id_vars=[
            "raw_date",
            "start_date_dt",
            "end_date_dt",
            "start_date",
            "end_date",
            "service_id",
            "dayofweek",
        ],
        var_name="cal_dayofweek",
        value_name="cal_val",
    )

    # map the calendar input strings to day of week integers to align w pandas
    # dayofweek output
    actual_service["cal_daynum"] = (
        actual_service["cal_dayofweek"].str.title().map(
            dict(zip(calendar.day_name, range(7)))
        )
    )
    # now check for rows that "work"
    # i.e., the day of week matches between datetime index & calendar input
    # and the datetime index is between the calendar row's start and end dates
    actual_service = actual_service[
        (actual_service.dayofweek == actual_service.cal_daynum)
        & (actual_service.start_date_dt <= actual_service.raw_date)
        & (actual_service.end_date_dt >= actual_service.raw_date)
    ]

    # now merge in calendar dates to the datetime index to get overrides
    actual_service = actual_service.merge(
        data.calendar_dates,
        how="outer",
        left_on=["raw_date", "service_id"],
        right_on=["date_dt", "service_id"],
    )

    # now add a service happened flag for dates where the schedule
    # indicates that this service occurred
    # i.e.: calendar has a service indicator of 1 and there's no
    # exception type from calendar_dates
    # OR calendar_dates has exception type of 1
    # otherwise no service
    # https://stackoverflow.com/questions/21415661/logical-operators-for-boolean-indexing-in-pandas
    actual_service["service_happened"] = (
        (actual_service["cal_val"] == "1")
        & (actual_service["exception_type"].isnull())
    ) | (actual_service["exception_type"] == "1")

    # now fill in rows where calendar_dates had a date outside the bounds of
    # the datetime index, so raw_date is always populated
    actual_service["raw_date"] = actual_service["raw_date"].fillna(
        actual_service["date_dt"]
    )

    # filter to only rows where service occurred
    service_happened = actual_service[actual_service.service_happened]

    # join trips to only service that occurred
    trips_happened = data.trips.merge(
        service_happened, how="left", on="service_id")

    # get only the trip / hour combos that actually occurred
    trip_stop_hours = data.stop_times.drop_duplicates(
        ["trip_id", "arrival_hour"]
    )
    # now join
    # result has one row per date + row from trips.txt (incl. route) + hour
    trip_summary = trips_happened.merge(
        trip_stop_hours, how="left", on="trip_id")

    # filter to only the rows for the period where this specific feed version was in effect
    trip_summary = trip_summary.loc[
        (trip_summary['raw_date'] >= feed_start_date)
        & (trip_summary['raw_date'] <= feed_end_date), :]

    return trip_summary


def group_trips(
    trip_summary: pd.DataFrame,
        groupby_vars: list) -> pd.DataFrame:
    """Generate summary grouped by groupby_vars

    Args:
        trip_summary (pd.DataFrame): A DataFrame of one trip per row i.e.
            the output of the make_trip_summary function.
        groupby_vars (list): Variables to group by.

    Returns:
        pd.DataFrame: A DataFrame with the trip count by groupby_vars e.g.
            route and date.
    """
    trip_summary = trip_summary.copy()
    summary = (
        trip_summary.groupby(by=groupby_vars)
        ["trip_id"]
        .nunique()
        .reset_index()
    )

    summary.rename(
        columns={
            "trip_id": "trip_count",
            "raw_date": "date"},
        inplace=True
    )
    summary.date = summary.date.dt.date
    return summary


def summarize_date_rt(trip_summary: pd.DataFrame) -> pd.DataFrame:
    """Summarize trips by date and route

    Args:
        trip_summary (pd.DataFrame): a summary of trips with one row per date.
            Output of the make_trip_summary function.

    Returns:
        pd.DataFrame: A DataFrame grouped by date and route
    """
    trip_summary = trip_summary.copy()
    groupby_vars = ["raw_date", "route_id"]

    # group to get trips by date by route
    route_daily_summary = group_trips(
        trip_summary,
        groupby_vars=groupby_vars,
    )

    return route_daily_summary


def make_linestring_of_points(
        sub_df: pd.DataFrame) -> shapely.geometry.LineString:
    """Draw a linestring from a DataFrame of points on a route

    Args:
        sub_df (pd.DataFrame): shapes data from CTA grouped by shape_id.

    Returns:
        shapely.geometry.LineString: A Shape constructed
            from points by shape_id.
    """
    sub_df = sub_df.copy()
    sorted_df = sub_df.sort_values(by="shape_pt_sequence")
    return shapely.geometry.LineString(list(sorted_df["pt"]))


def download_zip(version_id: str) -> zipfile.ZipFile:
    """Download a version schedule from transitfeeds.com

    Args:
        version_id (str): The version schedule in the form
            of a date e.g. YYYYMMDD

    Returns:
        zipfile.ZipFile: A zipfile for the CTA version id.
    """
    logger.info('Downloading CTA data')
    CTA_GTFS = zipfile.ZipFile(
        BytesIO(
            requests.get(
                f"https://transitfeeds.com/p/chicago-transit-authority"
                f"/165/{version_id}/download"
            ).content
        )
    )
    logging.info('Download complete')
    return CTA_GTFS


def download_extract_format(version_id: str) -> GTFSFeed:
    """Download a zipfile of GTFS data for a given version_id,
        extract data, and format date column.

    Args:
        version_id (str): The version of the GTFS schedule data to download

    Returns:
        GTFSFeed: A GTFSFeed object with formated dates
    """
    CTA_GTFS = download_zip(version_id)
    data = GTFSFeed.extract_data(CTA_GTFS, version_id=version_id)
    data = format_dates_hours(data)
    return data


def main() -> geopandas.GeoDataFrame:
    """Download data from CTA, construct shapes from shape data,
    and save to geojson file

    Returns:
        geopandas.GeoDataFrame: DataFrame with route shapes
    """

    schedule_list = create_schedule_list(5, 2022)
    # Get the latest version
    version_id = schedule_list[-1]['schedule_version']

    data = download_extract_format(version_id)

    # check that there are no dwell periods that cross hour boundary
    cross_hr_bndary = (
        data.stop_times[data.stop_times.arrival_hour !=
                        data.stop_times.departure_hour]
    )
    if not cross_hr_bndary.empty:
        logging.warn(
            f"There are dwell periods that cross "
            f"the hour boundary. See {cross_hr_bndary}")

    # ## Most common shape by route

    # get trip count by route, direction, shape id
    trips_by_rte_direction = (
        data.trips.groupby(["route_id", "shape_id", "direction"])["trip_id"]
        .count()
        .reset_index()
    )

    # keep only most common shape id by route, direction
    # follow: https://stackoverflow.com/a/54041328
    most_common_shapes = trips_by_rte_direction.sort_values(
        "trip_id").drop_duplicates(["route_id", "direction"], keep="last")

    # get additional route attributes
    most_common_shapes = most_common_shapes.merge(
        data.routes, how="left", on="route_id"
    )

    # make shapely points
    # https://www.geeksforgeeks.org/apply-function-to-every-row-in-a-pandas-dataframe/
    data.shapes["pt"] = data.shapes.apply(
        lambda row: shapely.geometry.Point(
            (float(row["shape_pt_lon"]), float(row["shape_pt_lat"]))
        ),
        axis=1,
    )

    data.shapes["shape_pt_sequence"] = pd.to_numeric(
        data.shapes["shape_pt_sequence"])

    # construct sorted list of shapely points
    # custom aggregation function: https://stackoverflow.com/a/10964938
    constructed_shapes = (data.shapes.groupby("shape_id").apply(
        make_linestring_of_points).reset_index())

    # merge in the other route attributes
    final = most_common_shapes.merge(
        constructed_shapes, how="left", on="shape_id")

    # make a "geometry" column for geopandas
    final["geometry"] = final[0]

    # construct the geopandas geodataframe
    final_gdf = geopandas.GeoDataFrame(data=final)

    # drop the column that's a list of shapely points
    final_gdf = final_gdf.drop(0, axis=1)

    # https://gis.stackexchange.com/questions/11910/meaning-of-simplifys-tolerance-parameter
    final_gdf["geometry"] = final_gdf["geometry"].simplify(0.0001)

    save_path = (
        Path(__file__).parent.parent / "data_output" / "scratch" /
        f"route_shapes_simplified_linestring"
        f"_{pendulum.now().strftime('%Y-%m-%d-%H:%M:%S')}.geojson"
    )
    with open(str(save_path), "w") as f:
        f.write(final_gdf.loc[(final_gdf["route_type"] == "3")].to_json())

    logging.info(f'geojson saved to {save_path}')

    return final_gdf


if __name__ == "__main__":
    main()
