"""
Utility functions common to both schedule and realtime analysis.
"""
from dataclasses import dataclass, field
from typing import List, Tuple

import pandas as pd


@dataclass
class AggInfo:
    """A class for storing information about
        aggregation of route and schedule data

    Args:
        freq (str, optional): An offset alias described in the Pandas
            time series docs. Defaults to None.
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
        aggvar (str, optional): variable to aggregate by.
            Defaults to trip_count
        byvars (List[str], optional): variables to passed to
            pd.DataFrame.groupby. Defaults to ['date', 'route_id'].
    """
    freq: str = 'D'
    aggvar: str = 'trip_count'
    byvars: List[str] = field(default_factory=lambda: ['date', 'route_id'])


def sum_by_frequency(
    df: pd.DataFrame,
        agg_info: AggInfo) -> pd.DataFrame:
    """Calculate total trips per route per frequency

    Args:
        df (pd.DataFrame): A DataFrame of route or scheduled route data
        agg_info (AggInfo): An AggInfo object describing how data
            is to be aggregated.

    Returns:
        pd.DataFrame: A DataFrame with the total number of trips per route
            by a specified frequency.
    """
    df = df.copy()
    out = (
        df.set_index(agg_info.byvars)
        .groupby(
            [pd.Grouper(level='date', freq=agg_info.freq),
                pd.Grouper(level='route_id')])[agg_info.aggvar]
        .sum().reset_index()
    )
    return out
