from data_analysis.plots import create_save_path
import pandas as pd

def test_create_save_path():
    result = create_save_path(save_name="save_name")
    assert "save_name" in result