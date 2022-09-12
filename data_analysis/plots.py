from pathlib import Path
from typing import Union

import matplotlib
matplotlib.use('QtAgg')

import logging
import folium
import geopandas as gpd
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns
import compare_scheduled_and_rt
import static_gtfs_analysis

CHICAGO_COORDINATES = (41.85, -87.68)
PLOTS_PATH = Path(__file__).parent.parent / 'plots' / 'scratch'

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def n_routes(df: Union[pd.DataFrame, gpd.GeoDataFrame],
             n: int = 10,
             lowest: bool = True) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Returns the n route_ids with the lowest or highest ratio
       of completed trips

    Args:
        df (Union[pd.DataFrame, gpd.GeoDataFrame]): DataFrame with ratio
            of completed trips by route_id
        n (int, optional): number of route_ids to return . Defaults to 10.
        lowest (bool, optional): whether to return the lowest ratios
    Returns:
        Union[pd.DataFrame, gpd.GeoDataFrame]: A DataFrame containing the
            n worst routes.
    """
    df = df.copy()
    if lowest:
        return df.sort_values(by="ratio").drop_duplicates(['route_id']).head(n)
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
        save_name (str, optional): _description_. Defaults to None.
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
        save_path = str(PLOTS_PATH / f'{save_name}.png')
        logger.info(f'Saving {save_path}')
        fig.get_figure().savefig(save_path)


def plot_map(
    geo_df: gpd.GeoDataFrame,
    var: str,
    save_name: str,
    cmap: str = None,
    background_map: folium.Map = None,
        save: bool = True) -> folium.Map:
    """ Create a map of bus routes from GeoDataFrame

    Args:
        geo_df (gpd.GeoDataFrame): DataFrame with bus routes, GPS coordinates,
           and some variable of interest e.g. ratio
        var (str): the variable of interest for a color scale e.g. ratio
        save_name (str): The name of the saved output map.
        cmap (str, optional): The color map to use for coloring the
           variable of interest. Defaults to None.
        background_map (folium.Map, optional): The background map for the
           bus routes in the DataFrame. Defaults to None.
        save (bool, optional): Whether to save the map. Defaults to True.

    Returns:
        folium.Map: A map of bus routes colored by a target variable.
    """
    geo_df = geo_df.copy()
    newmap = geo_df.explore(
        column=var,
        cmap=cmap,
        m=background_map,
        legend=True)
    if save:
        save_path = str(PLOTS_PATH / f'{save_name}_{var}.html')
        logger.info(f'Saving {save_path}')
        newmap.save(save_path)
    return newmap


def main() -> None:
    """Generate boxplot, maps of all routes, top 10 best routes, and
    top 10 worst routes
    """
    summary_df = compare_scheduled_and_rt.main()

    gdf = static_gtfs_analysis.main()

    summary_gdf = summary_df.merge(gdf, how="right", on="route_id")

    summary_gdf_geo = gpd.GeoDataFrame(summary_gdf)

    boxplot(x="day_type", y="ratio", data=summary_df, xlabel="Day Type",
            ylabel="Proportion of trips that occurred vs schedule")

    # Background map must be re-created to get a clear map between runs
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    plot_map(
        summary_gdf_geo,
        cmap='plasma',
        var="ratio",
        save_name="all_routes",
        background_map=chicago_map
    )

    # Worst performing routes
    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    worst_geo = n_routes(summary_gdf_geo)
    plot_map(
        worst_geo,
        cmap='winter',
        var="ratio",
        background_map=chicago_map,
        save_name="worst_routes"
    )

    chicago_map = folium.Map(location=CHICAGO_COORDINATES, zoom_start=10)
    best_geo = n_routes(summary_gdf_geo, lowest=False)
    plot_map(
        best_geo,
        cmap='winter',
        var="ratio",
        background_map=chicago_map,
        save_name="best_routes"
    )


if __name__ == '__main__':
    main()
