from typing import Union
import plotly.graph_objects
from data_analysis.plots import create_save_path, lineplot, n_worst_best_routes
import pandas as pd
import geopandas 

class MockFig:
    def add_trace(_, __):
        pass
    def write_html(_, __):
        pass
    def write_image(_, __):
        pass
    
def test_create_save_path():
    result = create_save_path(save_name="save_name")
    assert "save_name" in result

def test_lineplot(mocker):
    mocked_plot = mocker.patch('chart_studio.plotly.plot')

    lineplot(
        x="x",
        y="y",
        data=pd.DataFrame(
            {'x': [1, 2], 'y': [3, 4]}
        )
    )

    assert mocked_plot.assert_called_once
    assert mocked_plot.call_args[1]["filename"] == "lineplot_of_y_over_x"

def test_n_worst_best_routes(mocker):
    df = pd.DataFrame(
        {
            "Latitude": [-34.58, -15.78, -33.45, 4.60, 10.48],
            "Longitude": [-58.66, -47.91, -70.66, -74.08, -66.86],
            "p_percentiles": [1, 2000, 3000, 4, 5],
            "p": [1, 2000, 3000, 4, 5],
            "route_id": [1, 2, 300, 4, 5]
        }
    )
    gdf = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
    )
    best = (n_worst_best_routes(
        pd.concat([df, gdf]),
        'p',
        n=2,
        percentile=True,
        worst=False
    ))
    
    assert best.iloc[0]["route_id"] == 1