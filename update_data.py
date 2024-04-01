from collections import namedtuple
from argparse import ArgumentParser
import calendar
import datetime

import pandas as pd

import data_analysis.compare_scheduled_and_rt as csrt
import data_analysis.plots as plots
from data_analysis.cache_manager import CacheManager

DataUpdate = namedtuple(
    "DataUpdate", ["combined_long_df", "summary_df", "start_date", "end_date"]
)


def filter_dates(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Keep data between start_date and end_date (inclusive)

    Args:
        df (pd.DataFrame): A DataFrame with a 'date' column e.g.

            date        route_id  trip_count_rt  ...
        0  2022-05-20        1             43
        1  2022-05-20      100             33
        2  2022-05-20      103             80
        3  2022-05-20      106            121
        4  2022-05-20      108             34

        start_date (str): A date in 'YYYY-MM-DD' format.
            Must be on or after 2022-05-20
        end_date (str): A date in 'YYYY-MM-DD'. Must be on or before current date

    Returns:
        pd.DataFrame: A DataFrame filtered between start_date and end_date
    """
    df = df.copy()
    return df.loc[(df["date"] >= start_date) & (df["date"] <= end_date)]


def aggregate(
    df: pd.DataFrame, freq: str = "M", col_list: list = ["day_type", "route_id"]
) -> pd.DataFrame:
    """Sum columns in col_list by frequency freq.

    Args:
        df (pd.DataFrame): A DataFrame with the following contents
                date        day_type  trip_count_rt  trip_count_sched  ratio
            12 2022-06-01       wk          14908             18444  0.808285
            13 2022-06-02       wk          14602             18443  0.791737
            14 2022-06-04      sat           9533             13289  0.717360
            15 2022-06-05      sun           7624             10641  0.716474
            16 2022-06-06       wk          14713             18414  0.799012
        freq (str, optional): frequency of grouping the data e.g. daily,
            monthly, etc. Defaults to 'M'.
        col_list (list, optional): The columns to group by.
            Defaults to ['day_type', 'route_id'].

    Returns:
        pd.DataFrame: A DataFrame grouped by col_list summed by frequency freq.
                date        day_type     trip_type   count
            0  2022-06-30  Saturday  Actual Trips   38349
            1  2022-06-30    Sunday  Actual Trips   29879
            2  2022-06-30   Weekday  Actual Trips  291156
            3  2022-07-31   Holiday  Actual Trips    8326
            4  2022-07-31  Saturday  Actual Trips   28775
    """
    df.copy()

    df.loc[:, "date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    groupby_list = [pd.Grouper(freq=freq)] + col_list
    agg_day_type = df.groupby(groupby_list).sum()
    agg_day_type.drop(columns="ratio", inplace=True)
    agg_day_type = agg_day_type.reset_index()
    agg_day_type["date"] = agg_day_type["date"].astype(str)
    id_vars = ["date"] + col_list
    agg_day_type_melted = agg_day_type.melt(
        id_vars=id_vars,
        value_vars=["trip_count_rt", "trip_count_sched"],
        value_name="count",
        var_name="trip_type",
    )
    agg_day_type_melted["day_type"] = agg_day_type_melted["day_type"].map(
        plots.DAY_NAMES
    )
    agg_day_type_melted["trip_type"] = agg_day_type_melted["trip_type"].map(
        {"trip_count_rt": "Actual Trips", "trip_count_sched": "Scheduled Trips"}
    )

    return agg_day_type_melted


def update_interactive_map_data(data_update: DataUpdate) -> None:
    """Generate data for interactive map

    Args:
        data_update (DataUpdate): A DataUpdate object containing

            combined_long_df (pd.DataFrame): first part of tuple output
                from csrt.main.
            Example.

                    date        route_id  trip_count_rt  ...
            0  2022-05-20        1             43
            1  2022-05-20      100             33
            2  2022-05-20      103             80
            3  2022-05-20      106            121
            4  2022-05-20      108             34

            summary_df (pd.DataFrame): second part of tuple output from csrt.main
            Example.

            route_id day_type  trip_count_rt  trip_count_sched     ratio
            0        1      hol             47                59  0.796610
            1        1       wk           9872             11281  0.875100
            2      100      hol             38                53  0.716981
            3      100       wk           9281             10177  0.911958
            4      103      hol            475               552  0.860507

            start_date (str): A date in 'YYYY-MM-DD' format.
                Must be on or after 2022-05-20
    """
    combined_long_df = data_update.combined_long_df.copy()
    summary_df = data_update.summary_df.copy()
    start_date = data_update.start_date
    end_date = data_update.end_date

    # Remove 74 Fullerton bus from data
    combined_long_df = combined_long_df.loc[combined_long_df["route_id"] != "74"]
    summary_df = summary_df.loc[summary_df["route_id"] != "74"]

    route_daily_mean = (
        combined_long_df.groupby(["route_id"])["trip_count_rt"]
        .mean()
        .round(1)
        .reset_index()
    )

    route_daily_mean.rename(
        columns={"trip_count_rt": "avg_trip_count_rt"}, inplace=True
    )

    summary_df_mean = summary_df.merge(route_daily_mean, on="route_id")

    combined_long_df.loc[:, "date"] = pd.to_datetime(combined_long_df["date"])

    # Add ridership data to summary_df_mean
    ridership_by_rte_date = plots.fetch_ridership_data()

    ridership_end_date = ridership_by_rte_date["date"].max().strftime("%Y-%m-%d")

    merged_df = plots.merge_ridership_combined(
        combined_long_df=combined_long_df,
        ridership_df=ridership_by_rte_date,
        start_date=start_date,
        ridership_end_date=ridership_end_date,
    )

    daily_means_riders = plots.calculate_trips_per_rider(merged_df)

    # This is the average trip count corresponding to the ridership data,
    # which is usually a few months out of date. So we can drop it here and use
    # the up-to-date avg_trip_count_rt in summary_df_mean.

    daily_means_riders.drop(columns="avg_trip_count_rt", inplace=True)

    summary_df_mean = summary_df_mean.merge(daily_means_riders, on="route_id")

    # Skip route_id and day_type in the percentile and ranking calculations
    for col in summary_df_mean.columns[2:]:
        summary_df_mean = plots.calculate_percentile_and_rank(summary_df_mean, col=col)

    # JSON files for frontend interactive map by day type
    for day_type in plots.DAY_NAMES.keys():
        summary_df_mean_day = plots.filter_day_type(summary_df_mean, day_type=day_type)
        save_path = (
            plots.DATA_PATH / f"all_routes_{start_date}_to_{end_date}_{day_type}"
        )
        summary_df_mean_day.to_json(
            f"{save_path}.json", date_format="iso", orient="records"
        )
        summary_df_mean_day.to_html(f"{save_path}_table.html", index=False)


def update_lineplot_data(data_update: DataUpdate) -> None:
    """Refresh data for lineplots of bus performance over time

    Args:
        data_update (DataUpdate): A DataUpdate class containing

            combined_long_df (pd.DataFrame): first part of output of csrt.main
            Example.
                    date    route_id  trip_count_rt  ...
            0  2022-05-20        1             43
            1  2022-05-20      100             33
            2  2022-05-20      103             80
            3  2022-05-20      106            121
            4  2022-05-20      108             34

            start_date (str): A date in 'YYYY-MM-DD' format.
                Must be on or after 2022-05-20
            end_date (str): A date in 'YYYY-MM-DD'. Must be on or before current date

    """
    combined_long_df = data_update.combined_long_df.copy()
    start_date = data_update.start_date
    end_date = data_update.end_date

    # JSON files for lineplots
    json_cols = ["date", "trip_count_rt", "trip_count_sched", "ratio", "route_id"]

    combined_long_df[json_cols].to_json(
        plots.DATA_PATH / f"schedule_vs_realtime_all_day_types_routes_"
        f"{start_date}_to_{end_date}.json",
        date_format="iso",
        orient="records",
    )
    combined_long_df_wk = plots.filter_day_type(combined_long_df, "wk")

    combined_long_df_wk[json_cols].to_json(
        plots.DATA_PATH / f"schedule_vs_realtime_wk_routes"
        f"_{start_date}_to_{end_date}.json",
        date_format="iso",
        orient="records",
    )
    json_cols.pop()
    combined_long_groupby_date = plots.groupby_long_df(combined_long_df, "date")

    combined_long_groupby_date[json_cols].to_json(
        plots.DATA_PATH / f"schedule_vs_realtime_all_day_types_overall_"
        f"{start_date}_to_{end_date}.json",
        date_format="iso",
        orient="records",
    )

    combined_long_groupby_date_wk = plots.groupby_long_df(combined_long_df_wk, "date")

    combined_long_groupby_date_wk[json_cols].to_json(
        plots.DATA_PATH
        / f"schedule_vs_realtime_wk_overall_{start_date}_to_{end_date}.json",
        date_format="iso",
        orient="records",
    )


def update_barchart_data(
    data_update: DataUpdate, bar_start_date: str = "2022-06-01"
) -> None:
    """Refresh data for barcharts over time

    Args:
        data_update (DataUpdate): a DataUpdate object containing

            combined_long_df (pd.DataFrame): first part of output of csrt.main
        Example.
                date    route_id  trip_count_rt  ...
        0  2022-05-20        1             43
        1  2022-05-20      100             33
        2  2022-05-20      103             80
        3  2022-05-20      106            121
        4  2022-05-20      108             34

        bar_start_date (str, optional): The start date for bar plots.
            It should start at the beginning of the month to ensure
            a full month of data. Defaults to '2022-06-01'.
    """
    # JSON files for barcharts over time
    combined_long_df = data_update.combined_long_df.copy()

    combined_long_groupby_day_type = plots.groupby_long_df(
        combined_long_df, ["date", "day_type"]
    )

    last_month = plots.datetime.now().month - 1
    current_year = plots.datetime.now().year
    last_day = calendar.monthrange(current_year, last_month)[1]
    last_month_str = f"0{last_month}" if last_month < 10 else str(last_month)

    combined_long_groupby_day_type = filter_dates(
        combined_long_groupby_day_type,
        bar_start_date,
        f"{current_year}-{last_month_str}-{last_day}",
    )

    bar_end_date = combined_long_groupby_day_type["date"].astype(str).max()

    monthly_day_type_melted = aggregate(
        combined_long_groupby_day_type, col_list=["day_type"]
    )

    monthly_day_type_melted.to_json(
        plots.DATA_PATH / f"schedule_vs_realtime_barchart_by_day_type_"
        f"{bar_start_date}_to_{bar_end_date}.json",
        date_format="iso",
        orient="records",
    )

    combined_long_df_bardates = filter_dates(
        combined_long_df,
        bar_start_date,
        f"{current_year}-{last_month_str}-{last_day}",
    )

    monthly_day_type_melted_route = aggregate(combined_long_df_bardates)

    monthly_day_type_melted_route.to_json(
        plots.DATA_PATH / f"schedule_vs_realtime_barchart_by_day_type_routes_"
        f"{bar_start_date}_to_{bar_end_date}.json",
        date_format="iso",
        orient="records",
    )


class Updater:
    def __init__(self, previous_file):
        self.previous_df = pd.read_json(previous_file)

    # https://stackoverflow.com/questions/13703720/converting-between-datetime-timestamp-and-datetime64
    def latest(self):
        return pd.Timestamp(max(self.previous_df['date'].unique())).to_pydatetime()


def main() -> None:
    """Refresh data for interactive map, lineplots, and barcharts."""
    parser = ArgumentParser(
        prog='UpdateData',
        description='Update Ghost Buses Data',
    )
    parser.add_argument('--start_date', nargs=1, required=False, type=datetime.date.fromisoformat)
    parser.add_argument('--end_date', nargs=1, required=False, type=datetime.date.fromisoformat)
    parser.add_argument('--update', nargs=1, required=False, help="Update all-day comparison file.")
    parser.add_argument('--frequency', nargs=1, required=False,
                        help="Frequency as decribed in pandas offset aliases.")
    parser.add_argument('--recalculate', action='store_true',
                        help="Don't use the cache when calculating results.")
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.datetime.combine(args.start_date[0], datetime.time(), tzinfo=datetime.UTC)
    if args.end_date:
        end_date = datetime.datetime.combine(args.end_date[0], datetime.time(), tzinfo=datetime.UTC)

    existing_df = None
    if args.update:
        u = Updater(args.update[0])
        start_date = u.latest()
        existing_df = u.previous_df
    freq = 'D'
    if args.frequency:
        freq = args.frequency[0]
    cache_manager_args = {}
    if args.recalculate:
        cache_manager_args['ignore_cached_calculation'] = True
    if args.verbose:
        cache_manager_args['verbose'] = True
    cache_manager = CacheManager(**cache_manager_args)
    combined_long_df, summary_df = csrt.main(cache_manager, freq=freq, start_date=start_date, end_date=end_date, existing=existing_df)

    combined_long_df.loc[:, "ratio"] = (
        combined_long_df.loc[:, "trip_count_rt"]
        / combined_long_df.loc[:, "trip_count_sched"]
    )
    try:
        start_date = combined_long_df["date"].min().strftime("%Y-%m-%d")
        end_date = combined_long_df["date"].max().strftime("%Y-%m-%d")
    except AttributeError:
        start_date = combined_long_df["date"].min()
        end_date = combined_long_df["date"].max()

    data_update = DataUpdate(
        combined_long_df=combined_long_df,
        summary_df=summary_df,
        start_date=start_date,
        end_date=end_date,
    )

    update_interactive_map_data(data_update)
    update_lineplot_data(data_update)
    update_barchart_data(data_update)


if __name__ == "__main__":
    main()
