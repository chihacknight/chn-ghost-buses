import os
from dataclasses import dataclass, field
from typing import List
import logging

# required for pandas to read csv from aws
import s3fs
from s3path import S3Path
import pandas as pd
import pendulum
from tqdm import tqdm
from dotenv import load_dotenv


load_dotenv()

BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

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
        my_range: List[str] = ["2022-05-31", "2022-07-04"]) -> pd.DataFrame:
    """Calculate ratio of trips to scheduled trips per route
       per specified frequency.

    Args:
        rt_df (pd.DataFrame): A DataFrame of daily route data
        sched_df (pd.DataFrame): A DataFrame of daily scheduled route data
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.
        my_range (List[str], optional): The date range of schedule data.
            Defaults to ["2022-05-31", "2022-07-04"].

    Returns:
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
            [my_range[0], my_range[1]]), "day_type"
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

    return compare_by_day_type


# Read in pre-computed files of RT and scheduled data and compare!
def combine_real_time_rt_comparison(
    schedule_feeds: List[dict],
    agg_info: AggInfo,
    my_range: List[str] = ["2022-05-31", "2022-07-04"],
        save: bool = True) -> pd.DataFrame:
    """Generate a combined DataFrame with the realtime route comparisons

    Args:
        schedule_feeds (List[dict]): A list of dictionaries with the keys
             "schedule_version", "feed_start_date", and "feed_end_date"
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.
        my_range (List[str], optional): A custom date range for trips.
            Defaults to ['2022-05-31', '2022-07-04'].
        save (bool, optional): whether to save the csv file to s3 bucket.

    Returns:
        pd.DataFrame: Combined DataFrame of various schedule versions
            with totals per route by a specified frequency.
    """
    combined = pd.DataFrame()
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
        schedule_raw = pd.read_csv(
            (SCHEDULE_SUMMARY_PATH /
             f'schedule_route_daily_hourly_summary_v'
             f'{feed["schedule_version"]}.csv').as_uri())

        rt_raw = pd.DataFrame()
        date_pbar = tqdm(date_range)
        for day in date_pbar:
            date_str = day.to_date_string()
            pbar.set_description(
                f"Processing {date_str} at"
                f"{pendulum.now().to_datetime_string()}"
            )
            daily_data = pd.read_csv(
                (BASE_PATH / f"bus_hourly_summary_v2/{date_str}.csv").as_uri()
            )
            rt_raw = pd.concat([rt_raw, daily_data])

        # basic reformatting
        rt = rt_raw.copy()
        schedule = schedule_raw.copy()
        rt["date"] = pd.to_datetime(rt.data_date, format="%Y-%m-%d")
        rt["route_id"] = rt["rt"]
        schedule["date"] = pd.to_datetime(schedule.date, format="%Y-%m-%d")

        compare_by_day_type = sum_trips_by_rt_by_freq(
            rt_df=rt,
            sched_df=schedule,
            agg_info=agg_info,
            my_range=my_range
        )

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
        logger.info(f"Processing {feed['schedule_version']}")
        combined = pd.concat([combined, compare_by_day_type])

    return combined


def build_summary(
    combined_df: pd.DataFrame,
    date_range: List[str] = ["2022-05-20", "2022-07-20"],
        save: bool = True) -> pd.DataFrame:
    """Create a summary by route and day type

    Args:
        combined_df (pd.DataFrame): A DataFrame with all schedule versions
        date_range (List[str]):
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
             f"{date_range[0]}_to_{date_range[1]}.csv").as_uri()
        )
        summary.to_csv(
            outpath,
            index=False,
        )
    return summary


def main() -> pd.DataFrame:
    """Calculate the summary by route and day across multiple schedule versions

    Returns:
        pd.DataFrame: A DataFrame summary across
            versioned schedule comparisons.
    """
    schedule_feeds = [
        {
            "schedule_version": "20220507",
            "feed_start_date": "2022-05-20",
            "feed_end_date": "2022-06-02",
        },
        {
            "schedule_version": "20220603",
            "feed_start_date": "2022-06-04",
            "feed_end_date": "2022-06-07",
        },
        {
            "schedule_version": "20220608",
            "feed_start_date": "2022-06-09",
            "feed_end_date": "2022-07-08",
        },
        {
            "schedule_version": "20220709",
            "feed_start_date": "2022-07-10",
            "feed_end_date": "2022-07-17",
        },
        {
            "schedule_version": "20220718",
            "feed_start_date": "2022-07-19",
            "feed_end_date": "2022-07-20",
        },
    ]
    agg_info = AggInfo()
    combined_df = combine_real_time_rt_comparison(
        schedule_feeds,
        agg_info=agg_info,
        save=False)
    return build_summary(combined_df, save=False)


if __name__ == "__main__":
    main()
