import os
import logging

import boto3
import json
import pandas as pd
import pendulum

# use for dev, but don't deploy to Lambda:
# from dotenv import load_dotenv
# load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_PRIVATE = os.getenv("BUCKET_PRIVATE")
BUCKET_PUBLIC = os.getenv("BUCKET_PUBLIC")

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def combine_daily_files(date: str):
    """Combine raw JSON files returned by API into daily CSVs. 

    Args:
        date: Date string for which raw JSON files should be combined into CSVs.
    """
    s3 = boto3.resource("s3")

    for bucket_name in [BUCKET_PRIVATE, BUCKET_PUBLIC]:
        logging.info(f"processing data from {bucket_name}")
        bucket = s3.Bucket(bucket_name)
        objects = bucket.objects.filter(Prefix=f"bus_data/{date}")
        logging.info(f"loaded objects to process for {date}")

        data_list = []
        errors_list = []
        counter = 0
        for obj in objects:
            counter += 1
            if counter % 20 == 0:
                logger.info(f"processing object # {counter}") 
            obj_name = obj.key
            # https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
            obj_body = json.loads(obj.get()["Body"].read().decode("utf-8"))

            new_data = pd.DataFrame()
            new_errors = pd.DataFrame()

            # expect ~12 "chunks" per JSON
            for chunk in obj_body.keys():
                if "vehicle" in obj_body[chunk]["bustime-response"].keys():
                    new_data = pd.concat(
                        [
                        new_data, 
                            pd.DataFrame(
                                obj_body[chunk]["bustime-response"]["vehicle"]
                            ),
                        ],
                        ignore_index=True,
                    )
                if "error" in obj_body[chunk]["bustime-response"].keys():
                    new_errors = pd.concat(
                        [
                        new_errors,
                            pd.DataFrame(obj_body[chunk]["bustime-response"]["error"]),
                        ],
                        ignore_index=True,
                    )
                new_data["scrape_file"] = obj_name
                new_errors["scrape_file"] = obj_name

                data_list.append(new_data)
                errors_list.append(new_errors)

        data = pd.concat(data_list, ignore_index=True)
        errors = pd.concat(errors_list, ignore_index=True)

        if len(errors) > 0:
            error_key = f"bus_full_day_errors_v2/{date}.csv"
            logging.info(f"saving errors to {bucket}/{error_key}")
            bucket.put_object(
                Body=errors.to_csv(index=False),
                Key=error_key,
            )

        if len(data) > 0:
            # convert data time to actual datetime
            data["data_time"] = pd.to_datetime(
                    data["tmstmp"], format="%Y%m%d %H:%M"
                )

            data["data_hour"] = data.data_time.dt.hour
            data["data_date"] = data.data_time.dt.date

            data_key = f"bus_full_day_data_v2/{date}.csv"
            logging.info(f"saving data to {bucket}/{data_key}")
            bucket.put_object(
                Body=data.to_csv(index=False),
                Key=data_key,
            )


def lambda_handler(event, context):
    date = pendulum.yesterday("America/Chicago").to_date_string()
    combine_daily_files(date)
