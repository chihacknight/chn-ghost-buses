from pathlib import Path

import logging
import datetime

import pandas as pd
import requests
from io import BytesIO

DATA_DIR = Path(__file__).parent.parent / "data_output" / "scratch"


class CacheManager:
    def __init__(self, ignore_cached_calculation=False, verbose=False):
        self.data_dir: Path = DATA_DIR
        self.objects = {}
        self.ignore_cached_calculation = ignore_cached_calculation
        self.verbose = verbose

    def log(self, *args):
        if self.verbose:
            logging.info(args)

    def retrieve_object(self, name, func):
        obj = self.objects.get(name)
        if obj is None:
            obj = func()
            self.objects[name] = obj
        return obj

    def retrieve(self, subdir, filename: str, url: str) -> BytesIO:
        cache_dir = self.data_dir / subdir
        if not cache_dir.exists():
            cache_dir.mkdir()
        filepath = cache_dir / filename
        if filepath.exists():
            self.log(f'Retrieved cached {url} from {filename}')
            return BytesIO(filepath.open('rb').read())
        bytes_io = BytesIO(requests.get(url).content)
        with filepath.open('wb') as ofh:
            ofh.write(bytes_io.getvalue())
        self.log(f'Stored cached {url} in {filename}')
        return bytes_io

    @staticmethod
    def fix_dt_column(df, c):
        def fixer(x):
            if type(x) is not int:
                return pd.NaT
            return datetime.datetime.fromtimestamp(x / 1000).astimezone(datetime.UTC)
        df[c] = df[c].apply(fixer)
        return df

    def retrieve_calculated_dataframe(self, subdir, filename, func, dt_fields: list[str]) -> pd.DataFrame:
        cache_dir = self.data_dir / subdir
        if not cache_dir.exists():
            cache_dir.mkdir()
        filepath = cache_dir / filename
        csv = filename.endswith('.csv')
        if self.ignore_cached_calculation:
            self.log(f'Ignoring whether {subdir}/{filename} is in cache')
            return func()
        if filepath.exists():
            self.log(f'Retrieved {subdir}/{filename} from cache')
            if csv:
                logging.debug(f'Reading csv from {filepath}')
                df = pd.read_csv(filepath, low_memory=False)
            else:
                df = pd.read_json(filepath)
            assert type(df) is pd.DataFrame
            if df.empty:
                return pd.DataFrame()
            for c in dt_fields:
                df = self.fix_dt_column(df, c)
            return df
        self.log(f'Writing {subdir}/{filename} to cache')
        df = func()
        if csv:
            df.to_csv(filepath)
        else:
            df.to_json(filepath)
        return df
