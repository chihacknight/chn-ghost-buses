from typing import List
import logging

# required for pandas to read csv from aws
import s3fs
import os
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


# Read in pre-computed files of RT and scheduled data and compare!
def combine_real_time_rt_comparison(
    schedule_feeds: List[dict],
    my_range: List[str] = ["2022-05-31", "2022-07-04"],
    save: bool = True,
) -> pd.DataFrame:
    """Generate a combined DataFrame with the realtime route comparisons

    Args:
        schedule_feeds (List[dict]): A list of dictionaries with the keys
             "schedule_version", "feed_start_date", and "feed_end_date"
        my_range (List[str], optional): A custom date range for trips..
            Defaults to ['2022-05-31', '2022-07-04'].
        save (bool, optional): whether to save the csv file to s3 bucket.

    Returns:
        pd.DataFrame: _description_
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

        # get total by route by day
        rt_daily_by_rte = (
            rt.groupby(by=["date", "route_id"])["trip_count"]
            .sum()
            .reset_index()
        )
        sched_daily_by_rte = (
            schedule.groupby(by=["date", "route_id"])["trip_count"].sum()
            .reset_index()
        )

        compare_daily_by_rte = rt_daily_by_rte.merge(
            sched_daily_by_rte,
            how="inner",
            on=["date", "route_id"],
            suffixes=["_rt", "_sched"],
        )

        # compare by day of week
        compare_daily_by_rte["dayofweek"] = (
            compare_daily_by_rte["date"]
            .dt.dayofweek
        )
        compare_daily_by_rte["day_type"] = (
            compare_daily_by_rte["dayofweek"].map(
                {0: "wk", 1: "wk", 2: "wk", 3: "wk",
                 4: "wk", 5: "sat", 6: "sun"})
        )
        compare_daily_by_rte.loc[
            compare_daily_by_rte.date.isin(
                [my_range[0], my_range[1]]), "day_type"
        ] = "hol"

        compare_by_day_type = (
            compare_daily_by_rte.groupby(["route_id", "day_type"])[
                ["trip_count_rt", "trip_count_sched"]
            ]
            .sum()
            .reset_index()
        )

        compare_by_day_type["ratio"] = (
            compare_by_day_type["trip_count_rt"]
            / compare_by_day_type["trip_count_sched"]
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
    save: bool = True,
) -> pd.DataFrame:
    """Create a summary by route and day type

    Args:
        combined_df (pd.DataFrame): A DataFrame with all schedule versions
        date_range (List[str]):
        save (bool, optional): whether to save DataFrame to s3.
            Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame summary with the
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


if __name__ == "__main__":
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

    combined_df = combine_real_time_rt_comparison(schedule_feeds, save=False)
    summary_df = build_summary(combined_df, save=False)
