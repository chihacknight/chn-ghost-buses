import boto3
import json
import logging
import math
import os
import pandas as pd
import pendulum
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def scrape(routes_df, url):
    bus_routes = routes_df[routes_df.route_type == 3]
    response_json = json.loads('{}')
    for chunk in range(math.ceil(len(bus_routes) / 10)):
        chunk_routes = routes_df.iloc[[chunk*10 + i for i in range(10)],]
        route_query_string = chunk_routes.route_short_name.str.cat(sep=',')
        logger.info(f"Requesting routes: {route_query_string}")
        try:
            chunk_response = json.loads(requests.get(url + f"&rt={route_query_string}" + "&format=json").text)
            response_json[f"chunk_{chunk}"] = chunk_response
        except requests.RequestException as e:
            logger.error("Error calling API")
            logger.error(e)
    logger.info(f"Data fetched")
    return response_json

def lambda_handler(event, context):
    API_KEY = os.environ.get("CHN_GHOST_BUS_CTA_BUS_TRACKER_API_KEY")
    BUCKET_NAME = 'chn-ghost-buses-private'
    s3 = boto3.client('s3')
    api_url = f"http://www.ctabustracker.com/bustime/api/v2/getvehicles?key={API_KEY}"
    routes_df = pd.read_csv(s3.get_object(Bucket = BUCKET_NAME, Key = 'current_routes.txt')['Body'])
    logger.info(f"Loaded routes df")
    data = json.dumps(scrape(routes_df, api_url))
    logger.info(f"Saving data")
    t = pendulum.now('America/Chicago')
    s3.put_object(Bucket = BUCKET_NAME, Key = f"bus_data/{t.to_date_string()}/{t.to_time_string()}.json", Body = data)