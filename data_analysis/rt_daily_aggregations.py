import os
from typing import Tuple
import logging

import boto3
import json
import pandas as pd
import pendulum
from tqdm import tqdm
import typer
from dotenv import load_dotenv
# have to run from root directory for this to work 
from scrape_data import combine_daily_files

load_dotenv()
app = typer.Typer()

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

@app.command()
def combine_daily_totals(start_date: str, end_date: str):

    date_range = [
        d
        for d in pendulum.period(
            pendulum.from_format(start_date, "YYYY-MM-DD"),
            pendulum.from_format(end_date, "YYYY-MM-DD"),
        ).range("days")
    ]

    pbar = tqdm(date_range)
    for day in pbar:
        combine_daily_files.combine_daily_files(day.to_date_string(), ['chn-ghost-buses-private'])


if __name__ == "__main__":
    app()
