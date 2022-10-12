import logging

import boto3
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
def combine_daily_totals(
    start_date: str = typer.Argument(..., help = "Start date in format YYYY-MM-DD. Acceptable dates are any date between 2022-05-19 and yesterday (so, if today is 2022-10-11, can input any date up to 2022-10-10. Must be before end date."), 
    end_date: str =  typer.Argument(..., help = "End date in format YYYY-MM-DD. Acceptable dates are any date between 2022-05-19 and yesterday (so, if today is 2022-10-11, can input any date up to 2022-10-10. Must be after start date."), 
    bucket_type: str = typer.Argument(..., help = "Either 'public' or 'private' -- bucket to read data from. User running must have read access to the bucket in question, so general users should list 'public'.")):
    """
    Take all the raw JSON files for each date in a date range from one of the CHN ghost buses S3 buckets (private or public)
    and aggregate them into daily CSV files.
    TODO: Add save parameter so these can be saved locally rather than just back to the bucket.
    """
    
    date_range = [
        d
        for d in pendulum.period(
            pendulum.from_format(start_date, "YYYY-MM-DD"),
            pendulum.from_format(end_date, "YYYY-MM-DD"),
        ).range("days")
    ]

    pbar = tqdm(date_range)
    for day in pbar:
        combine_daily_files.combine_daily_files(day.to_date_string(), [f'chn-ghost-buses-{bucket_type}'])


if __name__ == "__main__":
    app()
