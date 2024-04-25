from pathlib import Path

import logging

import requests
from io import BytesIO


class CacheManager:
    DATA_DIR = Path(__file__).parent.parent / "data_output" / "scratch"

    def __init__(self, verbose=False):
        self.verbose = verbose

    def log(self, *args):
        if self.verbose:
            logging.info(*args)

    def retrieve(self, subdir: str, filename: str, url: str) -> BytesIO:
        """Retrieve data from the local filesystem cache or a remote URL.

        Args:
            subdir (str): subdirectory under DATA_DIR.
            filename (str): filename in subdir.
            url (str): fetch data from this URL if the file does not exist locally.

        Returns:
            BytesIO: buffer containing payload data.
        """
        cache_dir = self.DATA_DIR / subdir
        if not cache_dir.exists():
            cache_dir.mkdir()
        filepath = cache_dir / filename
        if filepath.exists():
            self.log(f'Retrieved cached {url} from {subdir}/{filename}')
            return BytesIO(filepath.open('rb').read())
        bytes_io = BytesIO(requests.get(url).content)
        with filepath.open('wb') as ofh:
            ofh.write(bytes_io.getvalue())
        self.log(f'Stored cached {url} in {subdir}/{filename}')
        return bytes_io