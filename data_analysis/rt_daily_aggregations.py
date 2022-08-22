import os
from typing import Tuple
import logging

import boto3
import json
import pandas as pd
import pendulum

from dotenv import load_dotenv

load_dotenv()

BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def compute_hourly_totals(
    start_data: str, end_date: str, bucket_type: str = None
) -> Tuple[pd.DataFrame]:
    """Aggregate route data to hourly totals.

    Args:
        start_data (str): starting date of the route
        end_date (str): ending date of route
        bucket_type (str, optional): which bucket to pull data from. If
            'public', BUCKET_PUBLIC is used. If 'private', BUCKET_PRIVATE.
            Defaults to None.

    Returns:
        Tuple[pd.DataFrame]: a tuple of the data, errors, and combined
            hourly summaries DataFrames.
    """
    hourly_summary_combined = pd.DataFrame()
    date_range = [
        d
        for d in pendulum.period(
            pendulum.from_format(start_date, "YYYY-MM-DD"),
            pendulum.from_format(end_date, "YYYY-MM-DD"),
        ).range("days")
    ]
    s3 = boto3.resource("s3")
    if bucket_type is None or bucket_type == "public":
        bucket = s3.Bucket(BUCKET_PUBLIC)
    else:
        bucket = s3.Bucket(bucket_type)

    for day in date_range:
        date_str = day.to_date_string()
        logger.info(
            f"Processing {date_str} at {pendulum.now().to_datetime_string()}")
        objects = bucket.objects.filter(Prefix=f"bus_data/{date_str}")

        logger.info(f"------- loading data at"
                    f"{pendulum.now().to_datetime_string()}")

        # load data
        data_dict = {}

        # Access denied for public bucket
        for obj in objects:
            # logger.info(f"loading {obj}")
            obj_name = obj.key
            # https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
            obj_body = json.loads(obj.get()["Body"].read().decode("utf-8"))
            data_dict[obj_name] = obj_body

        # parse data into actual vehicle locations and errors

        logger.info(f"------- parsing data at"
                    f"{pendulum.now().to_datetime_string()}")

        data = pd.DataFrame()
        errors = pd.DataFrame()

        # k, v here are filename: full dict of JSON
        for k, v in data_dict.items():
            # logger.info(f"processing {k}")
            filename = k
            new_data = pd.DataFrame()
            new_errors = pd.DataFrame()
            # expect ~12 "chunks" per JSON
            for chunk, contents in v.items():
                if "vehicle" in v[chunk]["bustime-response"].keys():
                    new_data = new_data.append(
                        pd.DataFrame(v[chunk]["bustime-response"]["vehicle"])
                    )
                if "error" in v[chunk]["bustime-response"].keys():
                    new_errors = new_errors.append(
                        pd.DataFrame(v[chunk]["bustime-response"]["error"])
                    )
            new_data["scrape_file"] = filename
            new_errors["scrape_file"] = filename
            data = data.append(new_data)
            errors = errors.append(new_errors)

        logger.info(f"------- saving data at"
                    f"{pendulum.now().to_datetime_string()}")

        if len(errors) > 0:
            bucket.put_object(
                Body=errors.to_csv(index=False),
                Key=f"bus_full_day_errors_v2/{date_str}.csv",
            )

        if len(data) > 0:
            # convert data time to actual datetime
            data["data_time"] = pd.to_datetime(
                data["tmstmp"], format="%Y%m%d %H:%M")

            data["data_hour"] = data.data_time.dt.hour
            data["data_date"] = data.data_time.dt.date

            bucket.put_object(
                Body=data.to_csv(index=False),
                Key=f"bus_full_day_data_v2/{date_str}.csv",
            )

            # combine vids into a set (drops duplicates):
            # https://stackoverflow.com/a/45925961
            hourly_summary = (
                data.groupby(["data_date", "data_hour", "rt", "des"])
                .agg({"vid": set, "tatripid": set, "tablockid": set})
                .reset_index()
            )
            # get number of vehicles per hour per route
            hourly_summary["vh_count"] = hourly_summary["vid"].apply(len)
            hourly_summary["trip_count"] = hourly_summary["tatripid"].apply(
                len)
            hourly_summary["block_count"] = hourly_summary["tablockid"].apply(
                len)

            bucket.put_object(
                Body=hourly_summary.to_csv(index=False),
                Key=f"bus_hourly_summary_v2/{date_str}.csv",
            )
            pd.concat([hourly_summary_combined, hourly_summary])
        return data, errors, hourly_summary


if __name__ == "__main__":
    start_date = "2022-07-17"
    end_date = "2022-08-07"

    compute_hourly_totals(start_date, end_date)
