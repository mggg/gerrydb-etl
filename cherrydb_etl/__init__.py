"""Common utilities for ETL scripts."""
import logging
import sys
from hashlib import sha256
from io import BytesIO
from typing import Tuple

import geopandas as gpd
import httpx

log = logging.getLogger()


def config_logger(logger: logging.Logger) -> None:
    """Configures a logger to write to `stderr`."""
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)


def download_dataframe_with_hash(url: str) -> Tuple[gpd.GeoDataFrame, str]:
    """Returns a (Geo)DataFrame and a file hash from a downloaded file."""
    log.info("Downloading %s...", url)
    response = httpx.get(url)
    response.raise_for_status()

    content = BytesIO(response.content)
    content_hash = sha256(content.getbuffer())
    log.info("Downloaded %s (SHA256: %s)", url, content_hash.hexdigest())
    return gpd.read_file(content), content_hash


def pathify(name: str) -> str:
    """Converts a pretty name to a root-level path."""
    return name.strip().lower().replace(" ", "-").replace(".", "")
