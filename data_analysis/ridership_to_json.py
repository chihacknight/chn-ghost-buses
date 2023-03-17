import os
from pathlib import Path
import pandas as pd
import calendar
import argparse

project_name = os.getenv('PROJECT_NAME', 'chn-ghost-buses')
current_dir = Path(__file__)
project_dir = next(
    p for p in current_dir.parents
    if p.name == f'{project_name}'
)

DATA_PATH = project_dir / 'data_output' / 'scratch'

ridership = pd.read_csv('https://data.cityofchicago.org/api/views/jyb9-n7fm/rows.csv?accessType=DOWNLOAD')


def get_latest_month_and_year(ridership: pd.DataFrame) -> tuple:
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
    ridership.loc[:, 'date'] = pd.to_datetime(ridership.loc[:, 'date'], format="%m/%d/%Y")
    latest_date = ridership['date'].max()
    return latest_date.month, latest_date.year


def ridership_to_json(ridership: pd.DataFrame, month: int = None, year: int = None) -> None:
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
    latest_month, latest_year = get_latest_month_and_year(ridership)
    if month is None:
        month = latest_month
    if year is None:
        year = latest_year
    month_name = calendar.month_name[month]
    ridership['date'] = pd.to_datetime(ridership.date, format="%m/%d/%Y")
    ridership.rename({'route': 'route_id'}, axis=1, inplace=True)
    df = ridership[(ridership['date'].dt.month == month) & (ridership['date'].dt.year == year)].copy()
    df['day_type'] = df.daytype.map({'W': 'weekday', 'A': 'sat', 'U': 'sun'})
    df.loc[df.date == '2022-07-04', 'day_type'] = 'hol'
    df[['route_id', 'date', 'day_type', 'rides']].to_json(DATA_PATH / f'daily_{month_name}_{year}_cta_ridership_data.json', orient = 'records')
    df_daytype_summary = df.groupby(by = ['route_id', 'day_type']).agg({'rides': ['mean', 'sum']}).reset_index()
    df_daytype_summary.columns = ['route_id', 'day_type', 'avg_riders', 'total_riders']
    df_daytype_summary.to_json(DATA_PATH / f'{month_name}_{year}_cta_ridership_data_day_type_summary.json', orient = 'records')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", type=int, help="Month of ridership data")
    parser.add_argument("--year", type=int, help="Year of ridership data")
    args = parser.parse_args()
    ridership_to_json(ridership=ridership, month=args.month, year=args.year)