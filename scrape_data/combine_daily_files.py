import os
import logging

import boto3
import json
import pandas as pd
import pendulum

# use for dev, but don't deploy to Lambda:
# from dotenv import load_dotenv
# load_dotenv()

BUCKET_PRIVATE = os.getenv('BUCKET_PRIVATE')
BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC')

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def combine_daily_files(
    date: str
):
    """Combine raw JSON files returned by API into daily CSVs. 

    Args:
        date: Date string for which raw JSON files should be combined into CSVs.
    """
    s3 = boto3.resource("s3")

    for bucket_name in [BUCKET_PRIVATE, BUCKET_PUBLIC]:
        bucket = s3.Bucket(bucket_name)
        objects = bucket.objects.filter(Prefix=f"bus_data/{date}")

        # load data
        data_dict = {}

        for obj in objects:
            obj_name = obj.key
            # https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
            obj_body = json.loads(obj.get()["Body"].read().decode("utf-8"))
            data_dict[obj_name] = obj_body

            data = pd.DataFrame()
            errors = pd.DataFrame()

            # k, v here are filename, full dict of JSON
            for k, v in data_dict.items():
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

            if len(errors) > 0:
                bucket.put_object(
                    Body=errors.to_csv(index=False),
                    Key=f"bus_full_day_errors_v2/{date}.csv",
                )

            if len(data) > 0:
                # convert data time to actual datetime
                data["data_time"] = pd.to_datetime(
                    data["tmstmp"], format="%Y%m%d %H:%M")

                data["data_hour"] = data.data_time.dt.hour
                data["data_date"] = data.data_time.dt.date

                bucket.put_object(
                    Body=data.to_csv(index=False),
                    Key=f"bus_full_day_data_v2/{date}.csv",
                )


def lambda_handler(event, context):
    date = pendulum.yesterday("America/Chicago").to_date_string()
    combine_daily_files(date)
