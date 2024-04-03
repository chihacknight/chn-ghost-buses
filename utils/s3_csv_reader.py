import pandas as pd
from pathlib import Path
import data_analysis.compare_scheduled_and_rt as csrt
from data_analysis.cache_manager import CacheManager

CACHE_MANAGER = CacheManager()

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
    cache_filename = f'{filename.stem}.csv'
    df = pd.read_csv(
            CACHE_MANAGER.retrieve(
                's3csv',
                cache_filename,
                f'https://{csrt.BUCKET_PUBLIC}.s3.us-east-2.amazonaws.com/{s3_filename}',
            ),
        low_memory=False
        )
    return df
    