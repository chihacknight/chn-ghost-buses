import pandas as pd
from pathlib import Path
import botocore
import data_analysis.compare_scheduled_and_rt as csrt

def read_csv(filename: str | Path) -> pd.DataFrame:    
    """Read pandas csv from S3 using multiple methods

    Args:
        filename (str | Path): file to download from S3.

    Returns:
        pd.DataFrame: A Pandas DataFrame from the S3 file.
    """
    if isinstance(filename, str):
        filename = Path(filename)
    # Use low_memory option to avoid warning about columns with mixed dtypes.
    try:
        df = pd.read_csv(filename.as_uri(), low_memory=False)
    except (botocore.exceptions.ConnectTimeoutError, botocore.exceptions.EndpointConnectionError):
        s3_filename = '/'.join(filename.parts[-2:])
        df = pd.read_csv(
            f'https://{csrt.BUCKET_PUBLIC}.s3.us-east-2.amazonaws.com/{s3_filename}',
            low_memory=False
        )
    return df