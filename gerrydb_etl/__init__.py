"""Common utilities for ETL scripts."""

import logging
import sys
from hashlib import sha256
from io import BytesIO
from typing import Tuple

import geopandas as gpd
import httpx
from gerrydb.schemas import ColumnKind, ColumnType
from pydantic import BaseModel, Field

COLUMN_TYPE_TO_PY_TYPE = {
    ColumnType.BOOL: bool,
    ColumnType.FLOAT: float,
    ColumnType.INT: int,
    ColumnType.STR: str,
}

log = logging.getLogger()


def config_logger(logger: logging.Logger) -> None:
    """Configures a logger to write to `stderr`."""
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)


def download_dataframe_with_hash(
    url: str, *args, **kwargs
) -> Tuple[gpd.GeoDataFrame, str]:
    """Returns a (Geo)DataFrame and a file hash from a downloaded file."""
    log.info("Downloading %s...", url)
    # The census has started blocking requests from httpx, so we need to pretend to be a browser.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/110.0.5481.77 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    response = httpx.get(url, headers=headers)
    response.raise_for_status()

    content = BytesIO(response.content)
    content_hash = sha256(content.getbuffer())
    log.info("Downloaded %s (SHA256: %s)", url, content_hash.hexdigest())
    return gpd.read_file(content, *args, **kwargs), content_hash


def pathify(name: str) -> str:
    """Converts a pretty name to a root-level path."""
    return name.strip().lower().replace(" ", "-").replace(".", "")


class ColumnConfig(BaseModel):
    """Import configuration for a column."""

    class Config:
        frozen = True

    source: str
    target: str
    aliases: list[str] = Field(default_factory=list)
    kind: ColumnKind
    type: ColumnType
    description: str


class TabularConfig(BaseModel):
    """Import configuration for a tabular dataset."""

    class Config:
        frozen = True

    columns: list[ColumnConfig]
    source_url: str | None = None

    def source_dtypes(self):
        """Returns Pandas type annotations for the source DataFrame."""
        return {
            column.source: COLUMN_TYPE_TO_PY_TYPE[column.type]
            for column in self.columns
            if column.type in COLUMN_TYPE_TO_PY_TYPE
        }
