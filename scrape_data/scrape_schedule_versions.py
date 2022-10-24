from typing import List, Tuple

from bs4 import BeautifulSoup
import requests
import pendulum
import logging
import calendar
import pandas as pd

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

BASE_URL = "https://transitfeeds.com"


def check_latest_rt_data_date() -> str:
    """Fetch the latest available date of real-time bus data

    Returns:
        str: A string of the latest date in YYYY-MM-DD format.
    """
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


def fetch_schedule_versions(month: int, year: int) -> List[pendulum.date]:
    """Get the schedule versions from transitfeeds.com from the most recent
       to specified month and year (inclusive). In case there are
       multiple schedules for a given month and year pair,
       all schedules will be fetched.

    Args:
        month (int): The month of interest
        year (int): The year of interest

    Returns:
        List[pendulum.date]: A list of unique schedule versions
    """
    link_list = []
    page = 1
    found = False
    while not found:
        logging.info(f" Searching page {page}")
        url = BASE_URL + f"/p/chicago-transit-authority/165?p={page}"
        response = requests.get(url).content
        soup = BeautifulSoup(response, "lxml")
        # List of dates from first row
        table = soup.find_all('table')
        for row in table[0].tbody.find_all('tr'):
            first_col = row.find_all('td')[0]
            date = pendulum.parse(first_col.text.strip(), strict=False)
            # Find schedules up to and including the specified date.
            if date.month == month and date.year == year:
                logging.info(
                    f" Found schedule for"
                    f" {calendar.month_name[date.month]} {date.year}"
                )
                logging.info(
                    f" Adding schedule for {calendar.month_name[date.month]}"
                    f" {date.day}, {date.year}"
                )
                link_list.append(first_col)
                found = True
                continue
            if found:
                break
            link_list.append(first_col)
        page += 1

    date_list = [s.text.strip() for s in link_list]
    # Check for duplicates. The presence of duplicates could mean
    # that the schedule was not in-effect.
    # See https://github.com/chihacknight/chn-ghost-buses/issues/30
    duplicates = pd.Series(date_list)[pd.Series(date_list).duplicated()].values
    if len(duplicates) > 0:
        logging.info(
            f" The duplicate schedule versions are"
            f" {set(duplicates)}. Check whether these were in-effect."
        )

    return sorted(
        set([pendulum.parse(date, strict=False).date() for date in date_list])
    )


def modify_data_collection_start(date_list: List[pendulum.date]) -> List[pendulum.date]:
    """Whether to modify the schedule version for the start of
        data collection on May 20, 2022

    Args:
        date_list (List[pendulum.date]): A list of dates in pendulum format

    Returns:
        List[pendulum.date]: A list of dates in pendulum format where the
            start date for schedule version 2022-05-07
            is 2022-05-19. This will ensure that the date
            ranges are valid i.e. starting with 2022-05-20 up to the day
            before the next schedule version.
    """
    # For schedule version 20220507, set the date to be May 19th 2022,
    # one day before the start of data collection. This will mean that
    # the start date will fall on 2022-05-20 in calculate_version_date_ranges
    for idx, date in enumerate(date_list):
        if date.month == 5 and date.day == 7 and date.year == 2022:
            date = pendulum.date(2022, 5, 19)
            date_list[idx] = date

    return date_list


def calculate_version_date_ranges(
    month: int,
    year: int,
        start2022: bool = True) -> Tuple[List[pendulum.date], List[Tuple[pendulum.date, pendulum.date]]]:
    """Get the start and end dates for each schedule version from the most
        recent version to the version specified by the month and year

    Args:
        month (int): month of interest
        year (int): year of interest
        start2022 (bool, optional): Whether to modify the
            start date of version 20220507 to reflect the start of
            real-time bus data collection. Defaults to True.

    Returns:
        Tuple[List[pendulum.date], List[Tuple[pendulum.date, pendulum.date]]]:
            A list of schedule versions and list of tuples for the
            start and end dates corresponding to those versions.
    """
    schedule_list = fetch_schedule_versions(month=month, year=year)
    if start2022:
        schedule_list = modify_data_collection_start(schedule_list)

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

    # Handle the current schedule version by setting the end date as the latest
    # available date for data.
    start_end_list.append(
        (schedule_list[-1].add(days=1), check_latest_rt_data_date())
    )
    return schedule_list, start_end_list


def create_schedule_list_dict(
    schedule_list: List[pendulum.date],
        start_end_list: List[Tuple[pendulum.date, pendulum.date]]) -> List[dict]:
    """Create a list of dictionaries with keys for the schedule_version,
       start_date, and end_date

    Args:
        schedule_list (List[pendulum.date]): A list of schedule versions from
            transitfeeds.com
        start_end_list (List[pendulum.date]): A list of start and end dates
            for each version

    Returns:
        List[dict]: A list of dictionaries with the start and end dates
            corresponding to each schedule version.
    """
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


def create_schedule_list(month: int, year: int, start2022: bool = True) -> List[dict]:
    """Return a list of dictionaries with start and end dates
       for each schedule version.

    Args:
        month (int): month of interest
        year (int): year of interest
        start2022 (bool, optional): Whether to modify the
            start date of version 20220507 to reflect the start of
            real-time bus data collection. Defaults to True.

    Returns:
        List[dict]: A list of dictionaries with the start and end dates
            corresponding to each schedule version.
    """
    schedule_list, start_end_list = calculate_version_date_ranges(
        month=month,
        year=year,
        start2022=start2022
    )
    return create_schedule_list_dict(
        schedule_list=schedule_list,
        start_end_list=start_end_list
    )
