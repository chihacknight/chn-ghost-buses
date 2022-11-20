import os
from dataclasses import dataclass, field
from typing import List, Tuple
import logging

# required for pandas to read csv from aws
import s3fs
from s3path import S3Path
import pandas as pd
import pendulum
from tqdm import tqdm
from dotenv import load_dotenv

import data_analysis.static_gtfs_analysis as static_gtfs_analysis
from scrape_data.scrape_schedule_versions import create_schedule_list

load_dotenv()

BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')
logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

BASE_PATH = S3Path(f"/{BUCKET_PUBLIC}")

SCHEDULE_RT_PATH = BASE_PATH / "schedule_rt_comparisons" / "route_level"
SCHEDULE_SUMMARY_PATH = BASE_PATH / "schedule_summaries" / "route_level"


@dataclass
class AggInfo:
    """A class for storing information about
        aggregation of route and schedule data

    Args:
        freq (str, optional): An offset alias described in the Pandas
            time series docs. Defaults to None.
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
        aggvar (str, optional): variable to aggregate by.
            Defaults to trip_count
        byvars (List[str], optional): variables to passed to
            pd.DataFrame.groupby. Defaults to ['date', 'route_id'].
    """
    freq: str = 'D'
    aggvar: str = 'trip_count'
    byvars: List[str] = field(default_factory=lambda: ['date', 'route_id'])


def make_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Make a summary of trips that actually happened. The result will be
        used as base data for further aggregations.

    Args:
        df (pd.DataFrame): A DataFrame read from bus_full_day_data_v2/{date}.

    Returns:
        pd.DataFrame: A summary of full day data by
            date, route, and destination.
    """
    df = df.copy()
    df = (
        df.groupby(["data_date", "rt"])
        .agg({"vid": set, "tatripid": set, "tablockid": set})
        .reset_index()
    )
    df["vh_count"] = df["vid"].apply(len)
    df["trip_count"] = df["tatripid"].apply(len)
    df["block_count"] = df["tablockid"].apply(len)
    return df


def sum_by_frequency(
    df: pd.DataFrame,
        agg_info: AggInfo) -> pd.DataFrame:
    """Calculate total trips per route per frequency

    Args:
        df (pd.DataFrame): A DataFrame of route or scheduled route data
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.

    Returns:
        pd.DataFrame: A DataFrame with the total number of trips per route
            by a specified frequency.
    """
    df = df.copy()
    return (
        df.set_index(agg_info.byvars)
        .groupby(
            [pd.Grouper(level='date', freq=agg_info.freq),
                pd.Grouper(level='route_id')])[agg_info.aggvar]
        .sum().reset_index()
    )


def sum_trips_by_rt_by_freq(
    rt_df: pd.DataFrame,
    sched_df: pd.DataFrame,
    agg_info: AggInfo,
        holidays: List[str] = ["2022-05-30", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25"]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate ratio of trips to scheduled trips per route
       per specified frequency.

    Args:
        rt_df (pd.DataFrame): A DataFrame of daily route data
        sched_df (pd.DataFrame): A DataFrame of daily scheduled route data
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.
        holidays (List[str], optional): List of holidays in analyzed period in YYYY-MM-DD format.
            Defaults to ["2022-05-31", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25"].

    Returns:
        pd.DataFrame: DataFrame a row per day per route with the number of scheduled and observed trips. 
        pd.DataFrame: DataFrame with the total number of trips per route
            by specified frequency and the ratio of actual trips to
            scheduled trips.
    """

    rt_df = rt_df.copy()
    sched_df = sched_df.copy()

    rt_freq_by_rte = sum_by_frequency(
        rt_df,
        agg_info=agg_info
    )

    sched_freq_by_rte = sum_by_frequency(
        sched_df,
        agg_info=agg_info
    )

    compare_freq_by_rte = rt_freq_by_rte.merge(
        sched_freq_by_rte,
        how="inner",
        on=["date", "route_id"],
        suffixes=["_rt", "_sched"],
    )

    # compare by day of week
    compare_freq_by_rte["dayofweek"] = (
        compare_freq_by_rte["date"]
        .dt.dayofweek
    )
    compare_freq_by_rte["day_type"] = (
        compare_freq_by_rte["dayofweek"].map(
            {0: "wk", 1: "wk", 2: "wk", 3: "wk",
                4: "wk", 5: "sat", 6: "sun"})
    )
    compare_freq_by_rte.loc[
        compare_freq_by_rte.date.isin(
            holidays), "day_type"
    ] = "hol"

    compare_by_day_type = (
        compare_freq_by_rte.groupby(["route_id", "day_type"])[
            ["trip_count_rt", "trip_count_sched"]
        ]
        .sum()
        .reset_index()
    )

    compare_by_day_type["ratio"] = (
        compare_by_day_type["trip_count_rt"]
        / compare_by_day_type["trip_count_sched"]
    )

    return compare_freq_by_rte, compare_by_day_type


# Read in pre-computed files of RT and scheduled data and compare!
def combine_real_time_rt_comparison(
    schedule_feeds: List[dict],
    schedule_data_list: List[dict],
    agg_info: AggInfo,
    holidays: List[str] = ["2022-05-31", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25"],
        save: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Generate a combined DataFrame with the realtime route comparisons

    Args:
        schedule_feeds (List[dict]): A list of dictionaries with the keys
             "schedule_version", "feed_start_date", and "feed_end_date"
        schedule_data_list (List[dict]): A list of dictionaries with a
            "schedule_version" key and "data" key with a value corresponding to
            the daily route summary for that version.
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.
        holidays (List[str], optional): List of holidays in analyzed period in YYYY-MM-DD format.
            Defaults to ["2022-05-31", "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-25"].
        save (bool, optional): whether to save the csv file to s3 bucket.

    Returns:
        pd.DataFrame: Combined DataFrame of various schedule versions
            with daily counts of observed and scheduled trip count by route.
        pd.DataFrame: Combined DataFrame of various schedule versions
            with totals per route by a specified frequency.
    """
    combined_grouped = pd.DataFrame()
    combined_long = pd.DataFrame()
    pbar = tqdm(schedule_feeds)
    for feed in pbar:
        start_date = feed["feed_start_date"]
        end_date = feed["feed_end_date"]
        date_range = [
            d
            for d in pendulum.period(
                pendulum.from_format(start_date, "YYYY-MM-DD"),
                pendulum.from_format(end_date, "YYYY-MM-DD"),
            ).range("days")
        ]
        pbar.set_description(
            f"Loading schedule version {feed['schedule_version']}"
        )

        schedule_raw = (
            next(
                data_dict["data"] for data_dict in schedule_data_list
                if feed["schedule_version"] == data_dict["schedule_version"]
            )
        )

        rt_raw = pd.DataFrame()
        date_pbar = tqdm(date_range)
        for day in date_pbar:
            date_str = day.to_date_string()
            pbar.set_description(
                f" Processing {date_str} at "
                f"{pendulum.now().to_datetime_string()}"
            )

            # Use low_memory option to avoid warning about columns
            # with mixed dtypes.
            daily_data = pd.read_csv(
                (BASE_PATH / f"bus_full_day_data_v2/{date_str}.csv")
                .as_uri(),
                low_memory=False
            )

            daily_data = make_daily_summary(daily_data)

            rt_raw = pd.concat([rt_raw, daily_data])

        # basic reformatting
        rt = rt_raw.copy()
        schedule = schedule_raw.copy()
        rt["date"] = pd.to_datetime(rt.data_date, format="%Y-%m-%d")
        rt["route_id"] = rt["rt"]
        schedule["date"] = pd.to_datetime(schedule.date, format="%Y-%m-%d")

        compare_freq_by_rte, compare_by_day_type = sum_trips_by_rt_by_freq(
            rt_df=rt,
            sched_df=schedule,
            agg_info=agg_info,
            holidays=holidays
        )

        compare_by_day_type['feed_version'] = feed['schedule_version']
        compare_freq_by_rte['feed_version'] = feed['schedule_version']

        if save:
            outpath = (
                (SCHEDULE_RT_PATH /
                 f'schedule_v{feed["schedule_version"]}_'
                 f"realtime_rt_level_comparison_"
                 f'{feed["feed_start_date"]}_to_'
                 f'{feed["feed_end_date"]}.csv').as_uri())
            compare_by_day_type.to_csv(
                outpath,
                index=False,
            )
        logger.info(f" Processing version {feed['schedule_version']}")
        combined_grouped = pd.concat([combined_grouped, compare_by_day_type])
        combined_long = pd.concat([combined_long, compare_freq_by_rte])

    return combined_long, combined_grouped


def build_summary(
    combined_df: pd.DataFrame,
        save: bool = True) -> pd.DataFrame:
    """Create a summary by route and day type

    Args:
        combined_df (pd.DataFrame): A DataFrame with all schedule versions
        save (bool, optional): whether to save DataFrame to s3.
            Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame summary across
            versioned schedule comparisons.
    """
    combined_df = combined_df.copy(deep=True)
    summary = (
        combined_df.groupby(["route_id", "day_type"])[
            ["trip_count_rt", "trip_count_sched"]
        ]
        .sum()
        .reset_index()
    )

    summary["ratio"] = summary["trip_count_rt"] / summary["trip_count_sched"]

    if save:
        outpath = (
            (SCHEDULE_RT_PATH /
             f"combined_schedule_realtime_rt_level_comparison_"
             f"{pendulum.now()}.csv").as_uri()
        )
        summary.to_csv(
            outpath,
            index=False,
        )
    return summary


def main(freq: str = 'D') -> Tuple[List[dict],pd.DataFrame, pd.DataFrame]:
    """Calculate the summary by route and day across multiple schedule versions

    Args:
        freq (str): Frequency of aggregation. Defaults to Daily.
    Returns:
        pd.DataFrame: A DataFrame of every day in the specified data with
        scheduled and observed count of trips.
        pd.DataFrame: A DataFrame summary across
            versioned schedule comparisons.
    """
    schedule_feeds = create_schedule_list(month=5, year=2022)

    schedule_data_list = []
    pbar = tqdm(schedule_feeds)
    for feed in pbar:
        schedule_version = feed["schedule_version"]
        pbar.set_description(
            f"Generating daily schedule data for "
            f"schedule version {schedule_version}"
        )
        logger.info(
            f"\nDownloading zip file for schedule version "
            f"{schedule_version}"
        )
        CTA_GTFS = static_gtfs_analysis.download_zip(schedule_version)
        logger.info("\nExtracting data")
        data = static_gtfs_analysis.GTFSFeed.extract_data(
            CTA_GTFS,
            version_id=schedule_version
        )
        data = static_gtfs_analysis.format_dates_hours(data)

        logger.info("\nSummarizing trip data")
        trip_summary = static_gtfs_analysis.make_trip_summary(data, 
            pendulum.from_format(feed['feed_start_date'], 'YYYY-MM-DD'), 
            pendulum.from_format(feed['feed_end_date'], 'YYYY-MM-DD'))

        route_daily_summary = (
            static_gtfs_analysis
            .summarize_date_rt(trip_summary)
        )

        schedule_data_list.append(
            {"schedule_version": schedule_version,
             "data": route_daily_summary}
        )
    agg_info = AggInfo(freq=freq)
    combined_long, combined_grouped = combine_real_time_rt_comparison(
        schedule_feeds,
        schedule_data_list=schedule_data_list,
        agg_info=agg_info,
        save=False)
    return combined_long, build_summary(combined_grouped, save=False)


if __name__ == "__main__":
    main()
