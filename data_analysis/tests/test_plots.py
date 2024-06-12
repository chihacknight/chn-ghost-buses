import plotly.graph_objects
from data_analysis.plots import create_save_path
from data_analysis.plots import lineplot
import pandas as pd

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
