import pandas as pd
import logging
import os
from pathlib import Path
#import data_analysis.compare_scheduled_and_rt as csrt
from data_analysis.cache_manager import CacheManager
from functools import partial

BUCKET_PUBLIC = os.getenv('BUCKET_PUBLIC', 'chn-ghost-buses-public')


csvfm = CacheManager()

def read_csv(filename: str | Path) -> pd.DataFrame:    
    """Read pandas csv from S3

    Args:
        filename (str | Path): file to download from S3.

    Returns:
        pd.DataFrame: A Pandas DataFrame from the S3 file.
    """
    if isinstance(filename, str):
        filename = Path(filename)
    s3_filename = '/'.join(filename.parts[-2:])
    memoized_filename = f'{filename.stem}.csv'
    #logging.info(f'Reading {filename} which is {s3_filename}')
    getter = partial(pd.read_csv, f'https://{BUCKET_PUBLIC}.s3.us-east-2.amazonaws.com/{s3_filename}', low_memory=False)
    return csvfm.retrieve_calculated_dataframe('s3csv', memoized_filename, getter, [])
    # df = pd.read_csv(
    #          f'https://{csrt.BUCKET_PUBLIC}.s3.us-east-2.amazonaws.com/{s3_filename}',
    #          low_memory=False
    #      )
    # return df
    