from datetime import datetime
import os
from pathlib import Path
from typing import Union, List

import matplotlib
matplotlib.use('QtAgg')

import logging
import folium
from branca.colormap import LinearColormap
import geopandas as gpd
import pandas as pd
import numpy as np
import mapclassify
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss


import data_analysis.compare_scheduled_and_rt as compare_scheduled_and_rt
import data_analysis.static_gtfs_analysis as static_gtfs_analysis
import plotly.express as px
import plotly.graph_objects as go
import chart_studio.plotly as py


CHICAGO_COORDINATES = (41.85, -87.68)

# Return the project root directory
# https://stackoverflow.com/questions/25389095/python-get-path-of-root-project-structure
project_name = os.getenv('PROJECT_NAME', 'chn-ghost-buses')
current_dir = Path(__file__)
project_dir = next(
    p for p in current_dir.parents
    if p.name == f'{project_name}'
)
PLOTS_PATH = project_dir / 'plots' / 'scratch'
DATA_PATH = project_dir / 'data_output' / 'scratch'

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

DAY_NAMES = {
        'wk': 'Weekday',
        'hol': 'Holiday',
        'sat': 'Saturday',
        'sun': 'Sunday'
    }


# https://stackoverflow.com/questions/52503899/
# format-round-numerical-legend-label-in-geopandas
def legend_formatter(
    df: gpd.GeoDataFrame,
    var: str,
        decimals: str = '0f') -> List[str]:
    """Format the bounds for categorical variable to remove brackets
        and add comma separators for large numbers

    Args:
        df (gpd.GeoDataFrame): data containing routes and GPS coordinates
        var (str): variable of interest to be plotted.
        decimals (str, optional): Number of decimal places
            for the number display. Defaults to 0f (0 places).

    Returns:
        List[str]: A list of formatted bounds to be placed in legend.
    """
    df = df.copy()
    q5 = mapclassify.Quantiles(df[var], k=5)
    upper_bounds = q5.bins
    bounds = []
    for index, upper_bound in enumerate(upper_bounds):
        if index == 0:
            lower_bound = df[var].min()
        else:
            lower_bound = upper_bounds[index-1]
        if np.isnan(upper_bound):
            upper_bound = df[var].max()

        # format the numerical legend here
        bound = f'{lower_bound:,.{decimals}} - {upper_bound:,.{decimals}}'
        bounds.append(bound)
    return bounds


def n_worst_best_routes(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    col: str,
    n: int = 10,
    percentile: bool = True,
        worst: bool = True) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Returns the n route_ids with the lowest or highest ratio
       of completed trips

    Args:
        df (Union[pd.DataFrame, gpd.GeoDataFrame]): DataFrame with ratio
            of completed trips by route_id
        col (str): The DataFrame column to sort by.
        n (int, optional): number of route_ids to return . Defaults to 10.
        percentile (bool, optional): whether to return routes at a
            certain percentile rank. Defaults to True.
        worst (bool, optional): whether to return the lowest ratios.
            Defaults to True.
    Returns:
        Union[pd.DataFrame, gpd.GeoDataFrame]: A DataFrame containing the
            n worst or best routes.
    """
    df = df.copy()
    if percentile:
        if n > 100 or n <= 0:
            raise ValueError("The accepted values for n are 0 < n <= 100")

    if worst:
        if percentile:
            return (
                df.loc[df[f'{col}_percentiles'] <= n/100]
            )
        else:
            # Drop duplicates so that there are n unique routes
            # instead of duplicated routes when taking direction into account.
            return (
                df.sort_values(by=f"{col}")
                .drop_duplicates('route_id').head(n)
            )
    else:
        if percentile:
            return (
                df.loc[df[f'{col}_percentiles'] >= (100-n)/100]
            )
        else:
            return (
                df.sort_values(by=f"{col}", ascending=False)
                .drop_duplicates('route_id').head(n)
            )


def create_save_path(save_name: str, dir_name: Path = PLOTS_PATH) -> str:
    """Generate a save path for files

    Args:
        save_name (str): the name of the file to save
        dir_name (Path, optional): the directory in which to save the file.
            Defaults to PLOTS_PATH.

    Returns:
        str: A full path with the directory and filename
    """
    return str(
        dir_name / f'{save_name}'
        f'_{datetime.now().strftime("%Y-%m-%d")}'
    )


def lineplot(
        x: str,
        y: str,
        data: pd.DataFrame,
        xlabel: str = None,
        ylabel: str = None,
        color_var: str = None,
        show: bool = False,
        title: str = None,
        save: bool = True,
        save_name: str = None,
        rolling_median: bool = False) -> None:
    """Plot a line plot using Plotly

    Args:
        x (str): x-axis variable
        y (str): y-axis variable
        data (pd.DataFrame): DataFrame for plot
        xlabel (str, optional): x-axis label. Defaults to None.
        ylabel (str, optional): y-axis label. Defaults to None.
        color_var (str, optional): variable to color lines. Defaults to None.
        show (bool, optional): whether to display plot in interactive session.
            Defaults to False.
        title (str, optional): plot title. Defaults to None.
        save (bool, optional): whether to save plot. Defaults to True.
        save_name (str, optional): name of saved plot file. Defaults to None.
        rolling_median (bool, optional): whether to add a line showing
            the rolling median. Defaults to False.
    """

    data = data.copy()
    fig = go.Figure()
    if color_var is not None:
        if color_var == 'day_type':
            labels = DAY_NAMES
            labels[color_var] = "Day Type"

            for day_type in data[color_var].unique():
                sub_df = data.loc[data[color_var] == day_type]
                x1 = sub_df[x].values
                y1 = sub_df[y].values

                # Use markers only for holidays and line+markers for weekends
                if day_type == 'hol':
                    fig.add_trace(go.Scatter(
                        x=x1,
                        y=y1,
                        name=labels[day_type],
                        mode='markers'
                    ))
                elif day_type == 'sat' or day_type == 'sun':
                    fig.add_trace(go.Scatter(
                        x=x1,
                        y=y1,
                        name=labels[day_type],
                        mode='lines+markers'
                    ))
                else:
                    fig.add_trace(go.Scatter(
                        x=x1,
                        y=y1,
                        name=labels[day_type]
                    ))
            fig.update_layout(
                title=title,
                xaxis_title=xlabel,
                yaxis_title=ylabel,
                legend_title="Day Type"
            )
        else:
            for val in data[color_var].unique():
                sub_df = data.loc[data[color_var] == val]
                x1 = sub_df[x].values
                y1 = sub_df[y].values

                fig.add_trace(go.Scatter(
                    x=x1,
                    y=y1,
                    name=val
                ))
            fig.update_layout(
                title=title,
                xaxis_title=xlabel,
                yaxis_title=ylabel
            )
    else:
        x1 = data[x].values
        y1 = data[y].values

        fig.add_trace(go.Scatter(
            x=x1,
            y=y1,
            showlegend=False
        ))
        if rolling_median:
            rolling_m = data.set_index(x)[y].rolling('7D').median().values
            fig.add_trace(go.Scatter(
                x=x1,
                y=rolling_m,
                name='Rolling 7-day median'
            ))
            fig.update_layout(
                title=title,
                xaxis_title=xlabel,
                yaxis_title=ylabel,
                showlegend=True
            )

    if show:
        fig.show()

    if save:
        if save_name is None:
            save_name = f'lineplot_of_{y}_over_{x}'
        save_path = create_save_path(save_name)
        logger.info(f'Saving to {save_path}')
        fig.write_html(f'{save_path}.html')
        fig.write_image(f'{save_path}.png')
        logger.info('Saving to Chart Studio')
        py.plot(fig, filename=f"{save_name}", auto_open=False)


def scatterplot(
    x: str,
    y: str,
    xlabel: str = None,
    ylabel: str = None,
    show: bool = False,
    save: bool = True,
    save_name: str = None,
        **kwargs: dict) -> None:
    """Scatter plot with OLS trendline

    Args:
        x (str): The predictor variable for the x-axis
        y (str): The predicted variable for the y-axis
        xlabel (str, optional): x-axis label. Defaults to None.
        ylabel (str, optional): y-axis label. Defaults to None.
        show (bool, optional): Whether to show plot when calling function.
            Defaults to False.
        save (bool, optional): Whether to save the plot. Defaults to True.
        save_name (str, optional): name of saved plot file. Defaults to None.
        **kwargs : additional keyword arguments passed to px.scatter
    """
    fig = px.scatter(x=x, y=y, labels={x: xlabel, y: ylabel}, **kwargs)

    for a in fig.layout.annotations:
        a.text = a.text.split('=')[1]
        a.text = DAY_NAMES[a.text]

    if show:
        fig.show()

    if save:
        if save_name is None:
            save_name = f'scatterplot_of_{y}_by_{x}'
        save_path = create_save_path(save_name)
        logger.info(f'Saving to {save_path}')
        fig.write_html(f'{save_path}.html')
        fig.write_image(f'{save_path}.png')
        logger.info('Saving to Chart Studio')
        py.plot(fig, filename=f"{save_name}", auto_open=False)


def boxplot(
        x: str,
        y: str,
        data: pd.DataFrame,
        xlabel: str = None,
        ylabel: str = None,
        show: bool = False,
        title: str = None,
        save: bool = True,
            save_name: str = None) -> None:
    """ Make a boxplot and save the figure

    Args:
        x (str): the variable to plot by
        y (str): the variable of interest e.g. ratio
        data (pd.DataFrame): DataFrame to plot with
        xlabel (str, optional): Label for x axis. Defaults to None.
        ylabel (str, optional): Label for y axis. Defaults to None.
        show (bool, optional): whether to show plot. Defaults to False.
        title (str, optional): Plot title. Defaults to None
        save (bool, optional): whether to save plot. Defaults to True.
        save_name (str, optional): Name of the saved boxplot. Defaults to None.
    """
    data = data.copy()
    if x == 'day_type':
        data.replace({x: DAY_NAMES}, inplace=True)
    labels = {}
    if xlabel is not None:
        labels[x] = xlabel
    if ylabel is not None:
        labels[y] = ylabel

    fig = px.box(
        data_frame=data,
        x=x,
        y=y,
        title=title,
        labels=labels
    )
    if show:
        fig.show()

    if save:
        if save_name is None:
            save_name = f'boxplot_of_{y}_by_{x}'
        save_path = create_save_path(save_name)
        logger.info(f'Saving to {save_path}')
        fig.write_html(f'{save_path}.html')
        fig.write_image(f'{save_path}.png')
        logger.info('Saving to Chart Studio')
        py.plot(fig, filename=f"{save_name}", auto_open=False)


def plot_map(
    geo_df: gpd.GeoDataFrame,
    save_name: str,
    save: bool = True,
        **kwargs: dict) -> folium.Map:
    """ Create a map of bus routes from GeoDataFrame

    Args:
        geo_df (gpd.GeoDataFrame): DataFrame with bus routes, GPS coordinates,
           and some variable of interest e.g. ratio
        save_name (str): The name of the saved output map.
        save (bool, optional): Whether to save the map. Defaults to True.
        **kwargs (dict): A dictionary of keyword arguments passed
            to GeoDataFrame.explore
    Returns:
        folium.Map: A map of bus routes colored by a target variable.
    """
    geo_df = geo_df.copy()
    # Convert dates to strings. Folium cannot handle datetime
    date_columns = geo_df.select_dtypes(include=["datetime64"]).columns
    geo_df[date_columns] = geo_df[date_columns].astype(str)
    newmap = geo_df.explore(**kwargs)
    if save:
        save_path = create_save_path(f'{save_name}')
        logger.info(f'Saving to {save_path}')
        newmap.save(f"{save_path}.html")
    return newmap


def fetch_ridership_data() -> pd.DataFrame:
    """Download all ridership data i.e. from 2001 to present.
    Note that the latest data is usually a few months behind.

    Returns:
        pd.DataFrame: A DataFrame with all ridership data up to latest
            available date.
    """
    logger.info("Fetching ridership data")

    ridership_by_rte_date = pd.read_csv(
        "https://data.cityofchicago.org/api/views/"
        "jyb9-n7fm/rows.csv?accessType=DOWNLOAD")

    ridership_by_rte_date.loc[:, 'date'] = pd.to_datetime(
        ridership_by_rte_date.loc[:, 'date'],
        infer_datetime_format=True
    )
    return ridership_by_rte_date


def create_ridership_map(mvp: bool = False) -> None:
    """Generate a map of ridership per route

    Args:
        mvp (bool, optional): Whether to generate the data and map
            used in the MVP. Defaults to False.
    """
    # Daily totals by route
    logger.info("Creating map of ridership")

    ridership_by_rte_date = fetch_ridership_data()

    if mvp:
        logger.info(
            "Keeping ridership data between 2022-05-20"
            " and 2022-07-31 for MVP"
        )
        # Date range from 2022-05-20 to 2022-07-31 used in MVP
        ridership_by_rte_date = (
            ridership_by_rte_date.loc[
                (ridership_by_rte_date['date'] >= "2022-05-20")
                & (ridership_by_rte_date['date'] <= "2022-07-31")
            ]
        )
    else:
        logger.info("Keeping ridership data from 2022-05-20 onward")
        ridership_by_rte_date = (
            ridership_by_rte_date.loc[
                (ridership_by_rte_date['date'] >= "2022-05-20")
            ]
        )

    start_date = ridership_by_rte_date['date'].min().strftime('%Y-%m-%d')
    end_date = ridership_by_rte_date['date'].max().strftime('%Y-%m-%d')

    # Total number of riders per route
    ridership_by_rte = (
        ridership_by_rte_date
        .groupby('route')
        .sum()
        .reset_index()
    )

    ridership_by_rte.rename(columns={'route': 'route_id'}, inplace=True)

    logger.info("Creating GeoDataFrame")

    gdf = static_gtfs_analysis.main()

    rider_gdf = ridership_by_rte.merge(gdf, on="route_id")

    rider_gdf_geo = gpd.GeoDataFrame(rider_gdf)

    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    bounds = legend_formatter(rider_gdf_geo, "rides")
    kwargs = {
        "cmap": "plasma",
        "column": "rides",
        "scheme": "Quantiles",
        "m": chicago_map,
        "legend_kwds": {
            'caption': 'Number of Riders',
            'colorbar': False,
            'labels': bounds
        },
        "legend": True,
        "categorical": False,
        "k": 5
    }
    logger.info("Creating map of ridership by route binned by quintile")
    _ = plot_map(
        rider_gdf_geo,
        save_name=f"all_routes_quantiles_{start_date}_to_{end_date}_{kwargs['column']}",
        **kwargs
    )

    # Remove key from kwargs
    kwargs.pop('scheme', None)
    kwargs.pop('k', None)
    kwargs['legend_kwds'].pop('labels', None)
    # Dictionary keys are popped when passed to explore,
    # so must be added again.
    kwargs['legend_kwds']['caption'] = 'Number of Riders'
    kwargs['legend_kwds']['colorbar'] = False

    # Background map must be re-created to get a clear map between runs
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    kwargs['m'] = chicago_map

    plotted_map = plot_map(
        rider_gdf_geo,
        save=False,
        save_name='',
        **kwargs
    )
    # Manually specify plasma colormap to avoid overlapping labels
    # See https://www.kennethmoreland.com/color-advice/
    # :~:text=Plasma,der%20Walt%20and%20Nathaniel%20Smith.
    kwargs['legend_kwds']['caption'] = 'Number of Riders'
    kwargs['legend_kwds']['colorbar'] = False
    lcm = LinearColormap(
        colors=[
            (13, 8, 135),
            (84, 2, 163),
            (139, 10, 165),
            (185, 50, 137),
            (219, 92, 104),
            (244, 136, 73),
            (254, 188, 43),
            (240, 249, 33)
        ],
        vmin=0,
        vmax=rider_gdf_geo[kwargs["column"]].max(),
        caption=kwargs["legend_kwds"]["caption"]
    )
    logger.info("Creating map of ridership totals by route")
    lcm.add_to(plotted_map)

    save_name = f'all_routes_numeric_{start_date}_to_{end_date}'

    save_path = create_save_path(f'{save_name}_{kwargs["column"]}')
    plotted_map.save(f"{save_path}.html")


def groupby_long_df(df: pd.DataFrame,
                    groupbyvars: Union[List[str], str]) -> pd.DataFrame:
    """Group DataFrame by groupbyvars

    Args:
        df (pd.DataFrame): combined_long output DataFrame from
            compare_scheduled_and_rt.main
        groupbyvars Union[List[str], str]: A column or list
            of columns to group by.

    Returns:
        pd.DataFrame: A DataFrame grouped by groupby
            vars and a trip ratio column.
    """
    df = df.copy()
    df.loc[:, 'date'] = pd.to_datetime(df.loc[:, 'date'])

    df = (
        df.groupby(groupbyvars)[['trip_count_rt', 'trip_count_sched']]
        .sum()
        .reset_index()
    )

    df['ratio'] = (
        df['trip_count_rt']
        / df['trip_count_sched']
    )

    return df


def calculate_percentile_and_rank(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Add a percentile and rank column to the DataFrame

    Args:
        df (pd.DataFrame): A summary DataFrame output from
            compare_scheduled_and_rt.main
        col (str): The column in the DataFrame to be ranked.

    Returns:
        pd.DataFrame: A summary DataFrame with the columns
            percentiles and rank added.
    """
    df = df.copy()
    df[f'{col}_percentiles'] = df[col].rank(
        pct=True
    )
    df[f'{col}_ranking'] = (
        df[col]
        .rank(method='dense', na_option='top', ascending=False)
    )
    return df


def make_map(
    summary_gdf_geo: gpd.GeoDataFrame,
    save_name: str,
        summary_kwargs: dict) -> None:
    """Plot a map of trip ratio from given DataFrame

    Args:
        summary_gdf_geo (gpd.GeoDataFrame): GeoDataFrame containing trip
            ratios and GPS coordinates of bus routes
        save_name (str): Name of the saved plot
        summary_kwargs (dict): A dictionary of keywords passed
            to GeoPandas.explore method
    """
    summary_gdf_geo = summary_gdf_geo.copy()

    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    summary_kwargs["m"] = chicago_map

    _ = plot_map(
            summary_gdf_geo,
            save_name=save_name,
            **summary_kwargs
        )


def plot_and_save(
    summary_gdf_geo: gpd.GeoDataFrame,
    summary_kwargs: dict,
        save_name: str) -> None:
    """Plot a map from the data input and save it

    Args:
        summary_gdf_geo (gpd.GeoDataFrame): A GeoDataFrame of the summary data
            from compare_scheduled_and_rt.main or from the saved csv of the
            MVP data in the data_output directory
        summary_kwargs (dict): A dictionary of keyword arguments passed to
            geopandas.GeoDataFrame.explore method
        save_name (str): The name of the saved map
    """
    save_name = f"{save_name}_{summary_kwargs['column']}"
    make_map(
        summary_gdf_geo=summary_gdf_geo,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    path_name = create_save_path(save_name, DATA_PATH)
    # Take only the columns related to summary_kwargs['column']
    # and those used in the map
    first_cols = summary_gdf_geo.columns[:2].tolist()
    last_cols = summary_gdf_geo.columns[-10:].to_list()
    kwargs_cols = summary_gdf_geo.columns[
        summary_gdf_geo.columns.str.startswith(summary_kwargs['column'])
    ].tolist()
    cols = first_cols + kwargs_cols + last_cols
    summary_gdf_geo[cols].to_file(f'{path_name}.json', driver='GeoJSON')
    summary_gdf_geo[cols].to_html(f'{path_name}_table.html', index=False)


def calculate_trips_per_rider(
    merged_df: pd.DataFrame,
        num_riders: int = 1000) -> pd.DataFrame:
    """Calculate the number of trips per rider and per num_riders riders

    Args:
        merged_df (pd.DataFrame): The output from merge_ridership_combined
            function
        num_riders (int, optional): Multiply result by num_riders to get the
            number of trips per num_riders. Defaults to 1000.

    Returns:
        pd.DataFrame: A DataFrame with additional columns for the number
            of trips per rider and the number of trips per num_riders riders.
    """
    daily_means = (
        merged_df.groupby(['route_id'])[['trip_count_rt', 'rides']]
        .mean()
        .round(1)
        .reset_index()
    )

    daily_means['trips_per_rider'] = (
        daily_means['trip_count_rt'] / daily_means['rides']
    )

    daily_means[f'trips_per_{num_riders}_riders'] = (
        (daily_means['trips_per_rider'] * num_riders).round(1)
    )

    daily_means.rename(
        columns={
            "trip_count_rt": "avg_trip_count_rt",
            "rides": "avg_ridership",
        }, inplace=True)

    return daily_means


def filter_day_type(df: pd.DataFrame, day_type: str) -> pd.DataFrame:
    """Filter DataFrame by type of day

    Args:
        df (pd.DataFrame): DataFrame containing a day_type column
        day_type (str): The type of day

    Raises:
        ValueError: An error is raised if day_type is not one of
            'wk', 'sat', 'sun', or 'hol'

    Returns:
        pd.DataFrame: A DataFrame containing only rows with day_type
    """
    df = df.copy()
    day_types = list(DAY_NAMES.keys())
    if day_type not in day_types:
        error_text = ", ".join(day_types[:-1]) + ', or ' + day_types[-1]
        raise ValueError(f"day_type must be one of {error_text}")
    return df.loc[df['day_type'] == day_type]


def set_day_type_suffix(df: pd.DataFrame) -> str:
    """Set the day_type to be included in the filename of the 
       saved output

    Args:
        df (pd.DataFrame): DataFrame containing a day_type column

    Returns:
        str: The day type found in the day_type column. Returns the string
            all_day_types when there is a mix of day types
    """
    df = df.copy()
    day_suffix = None
    for day_type in DAY_NAMES.keys():
        if (df['day_type']
                .loc[df['day_type'].notnull()] == day_type).all():
            day_suffix = day_type
    if day_suffix is None:
        day_suffix = 'all_day_types'
    return day_suffix


def make_all_maps(
    summary_gdf_geo: gpd.GeoDataFrame,
    start_date: str,
        end_date: str) -> None:
    """Make all maps from data specified in run_mvp or main

    Args:
        summary_gdf_geo (gpd.GeoDataFrame): A GeoDataFrame from the second
            element of the output from compare_scheduled_and_rt.main or from
            the saved summary_df csv file in the data_output directory
        start_date (str): The beginning of data collection
        end_date (str): The end of data collection or the end of available
            ridership data
    """
    logger.info("Creating map of all routes")
    summary_kwargs = {
        "cmap": "plasma",
        "column": "ratio",
        "legend_kwds": {
            "caption": "Ratio of Actual Trips to Scheduled Trips",
            "max_labels": 5
        },
        "legend": True,
    }
    day_suffix = set_day_type_suffix(summary_gdf_geo)

    logger.info(f"Using only {day_suffix} data")
    save_name = f"all_routes_{start_date}_to_{end_date}_{day_suffix}"
    plot_and_save(
        summary_gdf_geo=summary_gdf_geo,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    logger.info("Creating map of all routes by average trip count")
    summary_kwargs["column"] = "avg_trip_count_rt"
    summary_kwargs["legend_kwds"] = {
        "caption": "Daily average of actual trips",
        "max_labels": 5
    }

    save_name = f"all_routes_{start_date}_to_{end_date}_{day_suffix}"
    plot_and_save(
        summary_gdf_geo=summary_gdf_geo,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    logger.info("Creating map of all routes by average trip count per rider")
    summary_kwargs["column"] = "trips_per_1000_riders"
    summary_kwargs["legend_kwds"] = {
        "caption": "Trips per 1000 riders",
        "max_labels": 5
    }

    save_name = f"all_routes_{start_date}_to_{end_date}_{day_suffix}"
    plot_and_save(
        summary_gdf_geo=summary_gdf_geo,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    logger.info("Creating map of all routes binned by quintile")
    bounds = legend_formatter(summary_gdf_geo, "ratio", decimals="2f")

    summary_kwargs_quantiles = {
        "cmap": "plasma",
        "column": "ratio",
        "scheme": "Quantiles",
        "legend_kwds": {
            'caption': "Ratio of Actual Trips to Scheduled Trips",
            'colorbar': False,
            'labels': bounds
        },
        "legend": True,
        "categorical": False,
        "k": 5
    }

    make_map(
        summary_gdf_geo=summary_gdf_geo,
        save_name=f"all_routes_quantiles_{start_date}_to_{end_date}"
                  f"_{summary_kwargs_quantiles['column']}_{day_suffix}",
        summary_kwargs=summary_kwargs_quantiles
    )

    bounds_trip_count_rt = legend_formatter(
        summary_gdf_geo,
        var="avg_trip_count_rt"
    )
    summary_kwargs_quantiles["column"] = "avg_trip_count_rt"
    summary_kwargs_quantiles["legend_kwds"] = {
        "caption": "Daily average of actual trips",
        "colorbar": False,
        "labels": bounds_trip_count_rt
    }
    logger.info(
        f"Creating map of all routes for {summary_kwargs_quantiles['column']}"
        " binned by quintile"
    )

    make_map(
        summary_gdf_geo=summary_gdf_geo,
        save_name=f"all_routes_quantiles_{start_date}_to_{end_date}"
                  f"_{summary_kwargs_quantiles['column']}_{day_suffix}",
        summary_kwargs=summary_kwargs_quantiles
    )

    bounds_trips_per_1000_riders = legend_formatter(
        summary_gdf_geo,
        var="trips_per_1000_riders"
    )

    summary_kwargs_quantiles["column"] = "trips_per_1000_riders"
    summary_kwargs_quantiles["legend_kwds"] = {
        "caption": "Daily average of actual trips per 1000 riders",
        "colorbar": False,
        "labels": bounds_trips_per_1000_riders
    }

    logger.info(
        f"Creating map of all routes for {summary_kwargs_quantiles['column']}"
        " binned by quintile"
    )
    make_map(
        summary_gdf_geo=summary_gdf_geo,
        save_name=f"all_routes_quantiles_{start_date}_to_{end_date}"
                  f"_{summary_kwargs_quantiles['column']}_{day_suffix}",
        summary_kwargs=summary_kwargs_quantiles
    )

    logger.info("Creating map of worst performing routes")

    worst_geo = n_worst_best_routes(
        summary_gdf_geo,
        col="ratio",
        percentile=False
    )

    summary_kwargs['legend_kwds'] = {
        "caption": "Ratio of Actual Trips to Scheduled Trips"
    }
    summary_kwargs['cmap'] = 'winter'
    summary_kwargs['column'] = 'ratio'

    save_name = f"worst_routes_{start_date}_to_{end_date}_{day_suffix}"

    plot_and_save(
        summary_gdf_geo=worst_geo,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    worst_geo_trips = n_worst_best_routes(
        summary_gdf_geo,
        col="avg_trip_count_rt",
        percentile=False
    )

    summary_kwargs["legend_kwds"] = {"caption": "Daily average of actual trips"}
    summary_kwargs["cmap"] = "winter"
    summary_kwargs["column"] = "avg_trip_count_rt"

    logger.info(f"Creating map of worst performing routes by {summary_kwargs['column']}")

    save_name = f"worst_routes_{start_date}_to_{end_date}_{day_suffix}"
    plot_and_save(
        summary_gdf_geo=worst_geo_trips,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    logger.info("Worst geo trips per 1000 riders")
    worst_geo_trips_per_1000_riders = n_worst_best_routes(
        summary_gdf_geo,
        col="trips_per_1000_riders",
        percentile=False
    )

    summary_kwargs["legend_kwds"] = {
        "caption": "Daily average of actual trips per 1000 riders"
    }

    summary_kwargs["cmap"] = "winter"
    summary_kwargs["column"] = "trips_per_1000_riders"

    logger.info(f"Creating map of worst performing routes by {summary_kwargs['column']}")

    save_name = f"worst_routes_{start_date}_to_{end_date}_{day_suffix}"
    plot_and_save(
        summary_gdf_geo=worst_geo_trips_per_1000_riders,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    logger.info("Creating map of best performing routes")
    best_geo = n_worst_best_routes(
        summary_gdf_geo,
        col="ratio",
        percentile=False,
        worst=False
    )

    summary_kwargs['legend_kwds'] = {
        "caption": "Ratio of Actual Trips to Scheduled Trips"
    }
    summary_kwargs['column'] = 'ratio'

    save_name = f"best_routes_{start_date}_to_{end_date}_{day_suffix}"
    plot_and_save(
        summary_gdf_geo=best_geo,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    best_geo_trips = n_worst_best_routes(
        summary_gdf_geo,
        col="avg_trip_count_rt",
        worst=False,
        percentile=False
    )

    summary_kwargs["legend_kwds"] = {"caption": "Daily average of actual trips"}
    summary_kwargs["cmap"] = "winter"
    summary_kwargs["column"] = "avg_trip_count_rt"

    logger.info(f"Creating map of best performing routes by {summary_kwargs['column']}")

    save_name = f"best_routes_{start_date}_to_{end_date}_{day_suffix}"
    plot_and_save(
        summary_gdf_geo=best_geo_trips,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )

    logger.info("Best geo trips per 1000 riders")
    best_geo_trips_per_1000_riders = n_worst_best_routes(
        summary_gdf_geo,
        col="trips_per_1000_riders",
        worst=False,
        percentile=False
    )

    summary_kwargs["legend_kwds"] = {
        "caption": "Daily average of actual trips per 1000 riders"
    }

    summary_kwargs["cmap"] = "winter"
    summary_kwargs["column"] = "trips_per_1000_riders"

    logger.info(f"Creating map of best performing routes by {summary_kwargs['column']}")

    save_name = f"best_routes_{start_date}_to_{end_date}_{day_suffix}"
    plot_and_save(
        summary_gdf_geo=best_geo_trips_per_1000_riders,
        summary_kwargs=summary_kwargs,
        save_name=save_name
    )


def run_mvp() -> None:
    """Reproduce maps and JSONs from MVP launch on 2022-11-11.
    """
    logger.info("Creating GeoDataFrame")
    gdf = static_gtfs_analysis.main()

    logger.info(
            "Loading data for MVP."
            " Date range is between 2022-05-20 and 2022-11-05"
        )
    combined_long_df = pd.read_csv(
        DATA_PATH / 'combined_long_df_2022-11-06.csv'
    )
    combined_long_df.loc[:, 'date'] = pd.to_datetime(combined_long_df.loc[:, 'date'])

    start_date = combined_long_df['date'].min().strftime('%Y-%m-%d')
    end_date = combined_long_df['date'].max().strftime('%Y-%m-%d')

    summary_df = pd.read_csv(DATA_PATH / 'summary_df_2022-11-06.csv')

    # MVP is weekday only
    logger.info("Keeping only weekday data for MVP")
    summary_df_wk = filter_day_type(summary_df, day_type='wk')
    summary_df_wk = calculate_percentile_and_rank(summary_df_wk, col="ratio")

    # Weekday only
    ridership_by_rte_date = fetch_ridership_data()

    ridership_by_rte_date_wk = ridership_by_rte_date.loc[
        ridership_by_rte_date['daytype'] == 'W'
    ]
    combined_long_df_wk = combined_long_df.loc[
        combined_long_df['day_type'] == 'wk'
    ]

    ridership_end_date = ridership_by_rte_date['date'].max().strftime('%Y-%m-%d')

    merged_df_wk = merge_ridership_combined(
        combined_long_df=combined_long_df_wk,
        ridership_df=ridership_by_rte_date_wk,
        start_date=start_date,
        ridership_end_date=ridership_end_date
    )

    daily_means = calculate_trips_per_rider(merged_df_wk)

    summary_df_wk_mean = summary_df_wk.merge(daily_means, on='route_id')

    summary_df_wk_mean = calculate_percentile_and_rank(
        summary_df_wk_mean,
        col="avg_trip_count_rt"
    )

    summary_df_wk_mean = calculate_percentile_and_rank(
        summary_df_wk_mean,
        col="trips_per_1000_riders"
    )

    summary_gdf = summary_df_wk_mean.merge(gdf, how="right", on="route_id")

    summary_gdf_geo = gpd.GeoDataFrame(summary_gdf)

    make_all_maps(
        summary_gdf_geo=summary_gdf_geo,
        start_date=start_date,
        end_date=end_date
    )

    create_ridership_map(mvp=True)

    make_descriptive_plots(
        combined_long_df=combined_long_df_wk,
        summary_df=summary_df_wk_mean
    )


def main(day_type: str = None) -> None:
    """ Generate maps of all routes, top 10 best routes,
    top 10 worst routes, and ridership

    Args:
        day_type (str, optional): day_type to filter by. Defaults to None.
    """
    logger.info("Creating GeoDataFrame")
    gdf = static_gtfs_analysis.main()

    logger.info("Getting latest real-time and schedule comparison data")

    combined_long_df, summary_df = compare_scheduled_and_rt.main(freq='D')

    if day_type is not None:
        summary_df = filter_day_type(summary_df, day_type=day_type)

    summary_df = calculate_percentile_and_rank(summary_df, col="ratio")

    route_daily_mean = (
        combined_long_df
        .groupby(['route_id'])['trip_count_rt'].mean().round(1)
        .reset_index()
    )

    route_daily_mean.rename(
        columns={"trip_count_rt": "avg_trip_count_rt"},
        inplace=True
    )

    summary_df_mean = summary_df.merge(route_daily_mean, on='route_id')

    summary_df_mean = calculate_percentile_and_rank(
        summary_df_mean,
        col="avg_trip_count_rt"
    )

    summary_gdf = summary_df_mean.merge(gdf, how="right", on="route_id")

    summary_gdf_geo = gpd.GeoDataFrame(summary_gdf)

    combined_long_df.loc[:, 'date'] = pd.to_datetime(
        combined_long_df.loc[:, 'date']
    )

    start_date = combined_long_df['date'].min().strftime('%Y-%m-%d')
    end_date = combined_long_df['date'].max().strftime('%Y-%m-%d')

    ridership_by_rte_date = fetch_ridership_data()

    ridership_end_date = ridership_by_rte_date['date'].max().strftime('%Y-%m-%d')

    merged_df = merge_ridership_combined(
        combined_long_df=combined_long_df,
        ridership_df=ridership_by_rte_date,
        start_date=start_date,
        ridership_end_date=ridership_end_date
    )

    daily_means = calculate_trips_per_rider(merged_df)

    summary_df_mean = summary_df.merge(daily_means, on='route_id')

    summary_df_mean = calculate_percentile_and_rank(
        summary_df_mean,
        col="avg_trip_count_rt"
    )

    summary_df_mean = calculate_percentile_and_rank(
        summary_df_mean,
        col="trips_per_1000_riders"
    )

    summary_gdf = summary_df_mean.merge(gdf, how="right", on="route_id")

    summary_gdf_geo = gpd.GeoDataFrame(summary_gdf)

    make_all_maps(
        summary_gdf_geo=summary_gdf_geo,
        start_date=start_date,
        end_date=end_date
    )

    create_ridership_map()

    make_descriptive_plots(
        combined_long_df=combined_long_df,
        summary_df=summary_df
    )


def merge_ridership_combined(
    combined_long_df: pd.DataFrame,
    ridership_df: pd.DataFrame,
    start_date: str,
        ridership_end_date: str) -> pd.DataFrame:
    """Merge the combined_long_df and ridership data

    Args:
        combined_long_df (pd.DataFrame): The first element of the output from
            the compare_scheduled_and_rt.main or the saved csv file from
            the MVP in the data_output directory
        ridership_df (pd.DataFrame): Ridership data taken from the
            fetch_ridership function
        start_date (str): The beginning of data collection from
            combined_long_df
        ridership_end_date (str): The end of data collection from the
            ridership data

    Returns:
        pd.DataFrame: A merged DataFrame with matching dates for ridership
            and real-time data
    """

    combined_long_df_rider = combined_long_df.loc[
        (combined_long_df['date'] >= start_date)
        &
        (combined_long_df['date'] <= ridership_end_date)
    ]
    ridership_df = ridership_df.loc[
        (ridership_df['date'] >= start_date)
        & (ridership_df['date'] <= ridership_end_date)
    ]

    return ridership_df.merge(
        combined_long_df_rider,
        left_on=["date", "route"],
        right_on=["date", "route_id"],
    )


def make_descriptive_plots(
    combined_long_df: pd.DataFrame,
        summary_df: pd.DataFrame) -> None:
    """Plot linecharts, boxplots, scatterplots, etc. of the trip ratio

    Args:
        combined_long_df (pd.DataFrame): A DataFrame containing actual trips
            and scheduled trips per route per day
        summary_df (pd.DataFrame): A DataFrame of actual trips and scheduled
            trips per route
    """
    combined_long_df = combined_long_df.copy()

    combined_long_df.loc[:, 'date'] = pd.to_datetime(
        combined_long_df.loc[:, 'date']
    )

    start_date = combined_long_df['date'].min().strftime('%Y-%m-%d')
    end_date = combined_long_df['date'].max().strftime('%Y-%m-%d')

    combined_long_df.loc[:, "ratio"] = (
        combined_long_df.loc[:, "trip_count_rt"] / combined_long_df.loc[:, "trip_count_sched"]
    )

    combined_long_groupby_date = groupby_long_df(
        combined_long_df,
        'date'
    )

    combined_long_groupby_day_type = groupby_long_df(
        combined_long_df,
        ['date', 'day_type']
    )

    logger.info(
        f"Result of the ADF test is"
        f" {adfuller(combined_long_groupby_date['ratio'])}"
    )
    # p-value is 0.95 > 0.05. Fail to reject the null of non-stationarity.
    logger.info(
        f"Result of the KPSS test is"
        f" {kpss(combined_long_groupby_date['ratio'],regression='c', nlags='auto')}"
    )
    # p-value is 0.01 < 0.05. Reject the null hypothesis of stationarity.

    # Both tests indicate that the time series is probably non-stationary.

    logger.info("Creating lineplot with moving median")
    lineplot(
        data=combined_long_groupby_date,
        x='date',
        y='ratio',
        xlabel='Date',
        ylabel='Ratio of actual trips to scheduled trips',
        rolling_median=True,
        title=f'Trip ratios<br>{start_date} to {end_date}',
        save_name="lineplot_of_ratio_over_time_w_rolling_median"
    )

    logger.info("Creating line plot by day type")
    lineplot(
        data=combined_long_groupby_day_type,
        x="date",
        y="ratio",
        xlabel="Date",
        ylabel="Ratio of actual trips to scheduled trips",
        color_var="day_type",
        title=f"Trip ratios by Day Type<br>{start_date} to {end_date}",
        save_name="lineplot_of_ratio_over_time_by_day_type"
    )

    logger.info("Creating box plot by day type")
    boxplot(
        x="day_type",
        y="ratio",
        data=summary_df,
        xlabel="Day Type",
        ylabel="Ratio of actual trips to scheduled trips",
        title=f"Trip ratio distribution by Day Type<br>"
              f"{start_date} to {end_date}"
    )

    ridership_by_rte_date = fetch_ridership_data()
    ridership_end_date = ridership_by_rte_date['date'].max().strftime('%Y-%m-%d')

    ridership_by_rte_date_wk = ridership_by_rte_date.loc[
        ridership_by_rte_date['daytype'] == 'W'
    ]

    combined_long_df_wk = combined_long_df.loc[
        combined_long_df['day_type'] == 'wk'
    ]

    merged_df_wk = merge_ridership_combined(
        combined_long_df=combined_long_df_wk,
        ridership_df=ridership_by_rte_date_wk,
        start_date=start_date,
        ridership_end_date=ridership_end_date)

    merged_df = merge_ridership_combined(
        combined_long_df=combined_long_df,
        ridership_df=ridership_by_rte_date,
        start_date=start_date,
        ridership_end_date=ridership_end_date
    )

    logger.info("Scatterplot of trip ratio by ridership for weekdays")
    scatterplot(
        data_frame=merged_df_wk,
        x="rides",
        y="ratio",
        title=f"Scatterplot of ratio by rides<br>"
              f"{start_date} to {ridership_end_date}",
        xlabel="Ridership",
        ylabel="Ratio of actual trips to scheduled trips",
        trendline="ols",
        trendline_color_override="red",
        save_name=f"scatterplot_of_ratio_by_ridership_"
                  f"{start_date}_to_{ridership_end_date}_wk"
    )

    logger.info("Scatterplot of trip ratio by ridership for all day types")
    scatterplot(
        data_frame=merged_df,
        x="rides",
        y="ratio",
        facet_col="day_type",
        title=f"Scatterplot of ratio by rides<br>"
              f"{start_date} to {ridership_end_date}",
        xlabel="Ridership",
        ylabel="Ratio of actual trips to scheduled trips",
        trendline="ols",
        trendline_color_override="red",
        save_name=f"scatterplot_of_ratio_by_ridership_"
                  f"{start_date}_to_{ridership_end_date}"
                  f"_all_day_types"
    )
