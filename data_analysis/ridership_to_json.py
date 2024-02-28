import os
from pathlib import Path
import pandas as pd
import calendar
import typing
import typer
import json
import requests
import sys

project_name = os.getenv('PROJECT_NAME', 'chn-ghost-buses')
current_dir = Path(__file__)
project_dir = next(
    p for p in current_dir.parents
    if p.name == f'{project_name}'
)

DATA_PATH = project_dir / 'data_output' / 'scratch'

def check_existing_date() -> typing.Optional[typing.Tuple[int]]:
    """Fetch the date of the ridership data currently in main branch of 
    frontend repo.

    Returns:
        typing.Optional[typing.Tuple[int]]: A tuple of month and year of the date 
            of the data in main branch. Returns None if no data is found.
    """
    # Grab the latest date from GitHub. URL doesn't work until frontend 
    # PR #129 merged into main.
    url = 'https://github.com/chihacknight/ghost-buses-frontend/main/src/Routes/cta_ridership_data_day_type_summary.json'
    json_data = requests.get(url).json()
    if 'error' in json_data.keys():
        print(f"Data not found at {url}")
        return
    json_data_gh = json_data['payload']['blob']['rawLines'][0]
    website_date = json.loads(json_data_gh)['date']
    month, year = website_date.split(' ')
    month_int = list(calendar.month_name).index(month)
    year_int = int(year)
    return month_int, year_int
    

def get_latest_month_and_year(ridership_df: pd.DataFrame) -> tuple:
    """Return the most recent month and year of the ridership data

    Args:
        ridership (pd.DataFrame): ridership (pd.DataFrame): DataFrame of 
            ridership downloaded from
            'https://data.cityofchicago.org/api/views/jyb9-n7fm/rows.csv?accessType=DOWNLOAD'
            Example:
            route	date	daytype	rides
        0	3	01/01/2001	U	7354
        1	4	01/01/2001	U	9288
        2	6	01/01/2001	U	6048
        3	8	01/01/2001	U	6309
        4	9	01/01/2001	U	11207 

    Returns:
        tuple: A month, year tuple
    """
    ridership = ridership_df.copy()
    ridership.loc[:, 'date'] = pd.to_datetime(ridership.loc[:, 'date'], infer_datetime_format=True)
    latest_date = ridership['date'].max()
    return latest_date.month, latest_date.year


def ridership_to_json(ridership_df: pd.DataFrame, month: int = None, year: int = None) -> None:
    """
    Save ridership data to JSON for given month and year.
    Note that the data is typically a few months 
    behind the current date. Takes the latest available data if
    month or year are None.

    Args:
        ridership (pd.DataFrame): DataFrame of ridership downloaded from
            'https://data.cityofchicago.org/api/views/jyb9-n7fm/rows.csv?accessType=DOWNLOAD'
            Example:
            route	date	daytype	rides
        0	3	01/01/2001	U	7354
        1	4	01/01/2001	U	9288
        2	6	01/01/2001	U	6048
        3	8	01/01/2001	U	6309
        4	9	01/01/2001	U	11207
        month (int): Month of interest. Defaults to None
        year (int): Year of interest. Defaults to None
    """
    ridership = ridership_df.copy()
    latest_month, latest_year = get_latest_month_and_year(ridership)
    if month is None:
        month = latest_month
    if year is None:
        year = latest_year
    month_name = calendar.month_name[month]
    
    existing_date = check_existing_date()
    if isinstance(existing_date, tuple):
        existing_month, existing_year = existing_date
        if month == existing_month and year == existing_year:
            print("No new ridership data. Exiting now.")
            sys.exit(0)
        else:
            print("New ridership data available")
    else:
        print(f"Using ridership data for {month} {year}")

    # Holidays that are the same day every year
    hols = ['12/25', '07/04', '01/01']
    ridership.loc[ridership.date.str.contains('|'.join(hols)), 'day_type'] = 'hol'

    ridership['date'] = pd.to_datetime(ridership.date, format="%m/%d/%Y")
    ridership.rename({'route': 'route_id'}, axis=1, inplace=True)
    ridership['day_type'] = ridership.daytype.map({'W': 'weekday', 'A': 'sat', 'U': 'sun'})
    
    df = ridership[(ridership['date'].dt.month == month) & (ridership['date'].dt.year == year)].copy()
    
    df[['route_id', 'date', 'day_type', 'rides']].to_json(DATA_PATH / f'daily_{month_name}_{year}_cta_ridership_data.json', orient = 'records')
    df_daytype_summary = df.groupby(by = ['route_id', 'day_type']).agg({'rides': ['mean', 'sum']}).reset_index()
    df_daytype_summary.columns = ['route_id', 'day_type', 'avg_riders', 'total_riders']
    df_daytype_summary_json = df_daytype_summary.to_json(orient='records')
    full_json = {'date': f'{month_name} {year}'}
    full_json['data'] = json.loads(df_daytype_summary_json)
    with open(DATA_PATH / f'{month_name}_{year}_cta_ridership_data_day_type_summary.json', 'w') as outfile:
        json.dump(full_json, outfile)

app = typer.Typer()

@app.command()
def main(month: int = None, year: int = None) -> None:
    
    print("Loading data from data.cityofchicago.org")
    ridership_df = pd.read_csv(
        'https://data.cityofchicago.org/api/views/'
        'jyb9-n7fm/rows.csv?accessType=DOWNLOAD'
    )
    print("Done!")
    ridership_to_json(ridership_df=ridership_df, month=month, year=year)


if __name__ == '__main__':
    app()
    