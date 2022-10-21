from datetime import datetime
import os
from pathlib import Path
from typing import Union, List

import matplotlib
matplotlib.use('QtAgg')

import logging
import folium
import geopandas as gpd
import pandas as pd
import numpy as np
import mapclassify

import matplotlib.pyplot as plt
import seaborn as sns
import compare_scheduled_and_rt
import static_gtfs_analysis


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
    n: int = 10,
    percentile: bool = True,
        worst: bool = True) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Returns the n route_ids with the lowest or highest ratio
       of completed trips

    Args:
        df (Union[pd.DataFrame, gpd.GeoDataFrame]): DataFrame with ratio
            of completed trips by route_id
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
                df.drop_duplicates(['route_id'])
                .loc[df['percentiles'] <= n/100]
            )
        else:
            return (
                df.sort_values(by="ratio")
                .drop_duplicates(['route_id'])
                .head(n)
            )
    else:
        if percentile:
            return (
                df.drop_duplicates(['route_id'])
                .loc[df['percentiles'] >= (100-n)/100]
            )
        else:
            return (
                df.sort_values(by="ratio", ascending=False)
                .drop_duplicates(['route_id']).head(n)
            )


def boxplot(
        x: str, y: str, data: pd.DataFrame,
        xlabel: str = None, ylabel: str = None,
        xtickslabels: str = None, show: bool = False,
        save: bool = True, save_name: str = None) -> None:
    """ Make a boxplot and save the figure

    Args:
        x (str): the variable to plot by
        y (str): the variable of interest e.g. ratio
        data (pd.DataFrame): DataFrame to plot with
        xlabel (str, optional): Label for x axis. Defaults to None.
        ylabel (str, optional): Label for y axis. Defaults to None.
        xtickslabels (str, optional): Labels for x axis ticks.
            Defaults to None.
        show (bool, optional): whether to show plot. Defaults to False.
        save (bool, optional): whether to save plot. Defaults to True.
        save_name (str, optional): Name of the saved boxplot. Defaults to None.
    """
    sns.set_theme(style='ticks', palette='pastel')
    fig = sns.boxplot(x=x, y=y, data=data)
    if xlabel is not None:
        plt.xlabel(xlabel)
    if ylabel is not None:
        plt.ylabel(ylabel)
    fig.set_xticklabels(['holiday', 'weekday', 'sat', 'sun'])

    if show:
        plt.show()

    if save:
        if save_name is None:
            save_name = f'boxplot_of_{y}_by_{x}'
        save_path = str(
            PLOTS_PATH / f'{save_name}'
            f'_{datetime.now().strftime("%Y-%m-%d")}.png')
        logger.info(f'Saving {save_path}')
        fig.get_figure().savefig(save_path)


def plot_map(
    geo_df: gpd.GeoDataFrame,
    save_name: str,
    kwargs: dict,
        save: bool = True) -> folium.Map:
    """ Create a map of bus routes from GeoDataFrame

    Args:
        geo_df (gpd.GeoDataFrame): DataFrame with bus routes, GPS coordinates,
           and some variable of interest e.g. ratio
        save_name (str): The name of the saved output map.
        kwargs (dict): A dictionary of keyword arguments passed
            to GeoDataFrame.explore
        save (bool, optional): Whether to save the map. Defaults to True.

    Returns:
        folium.Map: A map of bus routes colored by a target variable.
    """
    geo_df = geo_df.copy()
    # Convert dates to strings. Folium cannot handle datetime
    date_columns = geo_df.select_dtypes(include=["datetime64"]).columns
    geo_df[date_columns] = geo_df[date_columns].astype(str)
    newmap = geo_df.explore(**kwargs)
    if save:
        save_path = str(
            PLOTS_PATH / f'{save_name}_{kwargs["column"]}'
            f'_{datetime.now().strftime("%Y-%m-%d")}.html')
        logger.info(f'Saving {save_path}')
        newmap.save(save_path)
    return newmap


def create_ridership_map():
    ridership_by_rte = pd.read_csv(
        "https://data.cityofchicago.org/api/views/"
        "jyb9-n7fm/rows.csv?accessType=DOWNLOAD")
    ridership_by_rte.date = pd.to_datetime(
        ridership_by_rte.date,
        infer_datetime_format=True
    )
    ridership_by_rte_date = (
        ridership_by_rte.set_index(['date', 'route']).groupby(
            [pd.Grouper(level='date', freq='A-MAY'),
             pd.Grouper(level='route')]
        ).sum().reset_index()
    )
    ridership_by_rte_date.rename(columns={'route': 'route_id'}, inplace=True)

    gdf = static_gtfs_analysis.main()

    rider_gdf = ridership_by_rte_date.merge(gdf, on="route_id")

    rider_gdf2022 = rider_gdf.loc[rider_gdf.date.dt.year == 2022]
    rider_gdf2022_geo = gpd.GeoDataFrame(rider_gdf2022)

    # Background map must be re-created to get a clear map between runs
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    bounds = legend_formatter(rider_gdf2022_geo, "rides")
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
        "k": 5}

    _ = plot_map(
        rider_gdf2022_geo,
        save_name='all_routes_categorical_2022-01-03_to_2022-05-31',
        **kwargs
    )

    # Remove key from kwargs
    kwargs.pop('scheme', None)
    kwargs.pop('k', None)
    kwargs['legend_kwds'].pop('labels', None)
    # Dictionary keys are popped when passed to explore,
    # so must be added again.
    kwargs['legend_kwds']['caption'] = 'Number of Riders'
    kwargs['legend_kwds']['max_labels'] = 4
    kwargs['legend_kwds']['colorbar'] = True
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    kwargs['m'] = chicago_map

    _ = plot_map(
        rider_gdf2022_geo,
        save_name='all_routes_numeric_2022-01-03_to_2022-05-31',
        **kwargs
    )


def main() -> None:
    """Generate boxplot, maps of all routes, top 10 best routes, and
    top 10 worst routes. Map of ridership
    """
    gdf = static_gtfs_analysis.main()

    summary_df = compare_scheduled_and_rt.main(freq='D')

    summary_gdf = summary_df.merge(gdf, how="right", on="route_id")

    summary_gdf_geo = gpd.GeoDataFrame(summary_gdf)

    summary_gdf_geo['percentiles'] = summary_gdf_geo['ratio'].rank(pct=True)

    boxplot(x="day_type", y="ratio", data=summary_df, xlabel="Day Type",
            ylabel="Proportion of trips that occurred vs schedule",
            xtickslabels=['holiday', 'weekday', 'sat', 'sun'])

    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    summary_kwargs = {
        "cmap": "plasma",
        "column": "ratio",
        "m": chicago_map,
        "legend_kwds": {"caption": "Ratio of Actual Trips to Scheduled Trips"},
        "legend": True,
    }
    _ = plot_map(
            summary_gdf_geo,
            save_name="all_routes_2022-05-20_to_2022-07-20",
            **summary_kwargs
        )

    # Quantile plot
    bounds = legend_formatter(summary_gdf_geo, "ratio", decimals="2f")
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    summary_kwargs_quantiles = {
        "cmap": "plasma",
        "column": "ratio",
        "scheme": "Quantiles",
        "m": chicago_map,
        "legend_kwds": {
            'caption': "Ratio of Actual Trips to Scheduled Trips",
            'colorbar': False,
            'labels': bounds
        },
        "legend": True,
        "categorical": False,
        "k": 5
    }

    _ = plot_map(
            summary_gdf_geo,
            save_name='all_routes_quantiles_2022-05-20_to_2022-07-20',
            **summary_kwargs_quantiles
        )

    
    # Worst performing routes
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    worst_geo = n_worst_best_routes(summary_gdf_geo)
    worst_geo.to_file(
        str(DATA_PATH / 'worst_routes_2022-05-20'
            '_to_2022-07-20.geojson'), driver="GeoJSON"
    )
    summary_kwargs['legend_kwds'] = {
        "caption": "Ratio of Actual Trips to Scheduled Trips"
    }
    summary_kwargs['cmap'] = 'winter'
    summary_kwargs['m'] = chicago_map
    _ = plot_map(
            worst_geo,
            save_name="worst_routes_2022-05-20_to_2022-07-20",
            **summary_kwargs
        )

    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    best_geo = n_worst_best_routes(summary_gdf_geo, worst=False)
    best_geo.to_file(
        str(DATA_PATH / 'best_routes_2022-05-20'
            '_to_2022-07-20.geojson'), driver="GeoJSON"
    )

    summary_kwargs['legend_kwds'] = {
        "caption": "Ratio of Actual Trips to Scheduled Trips"
    }
    summary_kwargs['m'] = chicago_map
    _ = plot_map(
            best_geo,
            save_name="best_routes_2022-05-20_to_2022-07-20",
            **summary_kwargs
        )

    summary_gdf_geo.to_file(
        str(DATA_PATH / 'all_routes_2022-05-20'
            '_to_2022-07-20.geojson'), driver='GeoJSON'
    )


if __name__ == '__main__':
    main()
