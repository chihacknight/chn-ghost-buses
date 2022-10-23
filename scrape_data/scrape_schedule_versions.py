from typing import List, Tuple
from bs4 import BeautifulSoup
import requests
import pendulum

BASE_URL = "https://transitfeeds.com"

# TODO Generalize for historical feeds


def check_latest_rt_data_date():
    if pendulum.now("America/Chicago").hour >= 11:
        end_date = (
            pendulum.yesterday("America/Chicago")
            .date().format('YYYY-MM-DD')
        )
    else:
        end_date = (
            pendulum.now("America/Chicago").subtract(days=2)
            .date().format('YYYY-MM-DD')
        )
    return end_date


def fetch_schedule_versions() -> List[pendulum.date]:
    link_list = []
    for page in range(1, 3):
        url = BASE_URL + f"/p/chicago-transit-authority/165?p={page}" 
        response = requests.get(url).content
        soup = BeautifulSoup(response, "lxml")
        # List of dates from first row
        table = soup.find_all('table')
        for row in table[0].tbody.find_all('tr'):
            first_col = row.find_all('td')[0]
            link_list.append(first_col)

    date_list = [s.text.strip() for s in link_list]

    return sorted(
        [pendulum.parse(date, strict=False).date() for date in date_list]
    )


def may2022_preproccessing(date_list: List[pendulum.date]) -> List[pendulum.date]:
    schedule_list = [
        date for date in date_list
        if date.month >= 5 and date.year == 2022
    ]

    # For schedule version 20220507, set the start date to be May 19th 2022,
    # one day before the start of data collection
    for idx, date in enumerate(schedule_list):
        if date.month == 5 and date.day == 7 and date.year == 2022:
            date = pendulum.date(2022, 5, 19)
            schedule_list[idx] = date

    return schedule_list


def calculate_version_date_ranges(
        start2022: bool = True) -> Tuple[List[pendulum.date], List[tuple]]:
    schedule_list = fetch_schedule_versions()
    if start2022:
        schedule_list = may2022_preproccessing(schedule_list)

    start_end_list = []
    for i in range(len(schedule_list)):
        try:
            date_tuple = (
                schedule_list[i].add(days=1),
                schedule_list[i+1].subtract(days=1)
            )
            start_end_list.append(date_tuple)
        except IndexError:
            pass

    # Handle the current schedule by setting the end date as the latest
    # available date for data.
    start_end_list.append(
        (schedule_list[-1].add(days=1), check_latest_rt_data_date())
    )
    return schedule_list, start_end_list


def create_schedule_list_dict(
    schedule_list: List[pendulum.date],
        start_end_list: List[pendulum.date]) -> List[dict]:
    schedule_list_dict = []
    for version, (start_date, end_date) in zip(schedule_list, start_end_list):
        # Changing back the starting version to 20220507
        if version == pendulum.date(2022, 5, 19):
            version = pendulum.date(2022, 5, 7)
        schedule_dict = {
            "schedule_version": version.format("YYYYMMDD"),
            "feed_start_date": start_date.format("YYYY-MM-DD"),
            "feed_end_date": end_date.format("YYYY-MM-DD")
        }
        schedule_list_dict.append(schedule_dict)
    return schedule_list_dict


def create_schedule_list() -> List[dict]:
    schedule_list, start_end_list = calculate_version_date_ranges()
    return create_schedule_list_dict(
        schedule_list=schedule_list,
        start_end_list=start_end_list
    )

