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
