"""Imports base Census geographies."""
import logging
from pathlib import Path

import click
from cherrydb import CherryDB

from cherrydb_etl import config_logger

log = logging.getLogger()


NOTES = """Loaded by"""
LEVELS = (
    ### central spine ###
    "block",
    "bg",
    "tract",
    "county",
    "state",
    ### auxiliary to spine ###
    "vtd",
    "place",
    "cousub",
    "aiannh",  # American Indian/Alaska Native/Native Hawaiian Areas
)

LAYER_URLS = {
    "block/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TABBLOCK/2010/tl_2020_{fips}_tabblock10.zip",
    "block/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TABBLOCK/2020/tl_2020_{fips}_tabblock20.zip",
    "bg/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/BG/2010/tl_2020_{fips}_bg10.zip",
    "bg/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/BG/2020/tl_2020_{fips}_bg20.zip",
    "tract/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TRACT/2010/tl_2020_{fips}_tract10.zip",
    "tract/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TRACT/2020/tl_2020_{fips}_tract20.zip",
    "county/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/COUNTY/2010/tl_2020_{fips}_county10.zip",
    "county/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/COUNTY/2020/tl_2020_{fips}_county20.zip",
    "state/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/STATE/2010/tl_2020_{fips}_state10.zip",
    "state/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/STATE/2020/tl_2020_{fips}_state20.zip",
    "vtd/2010": "https://www2.census.gov/geo/tiger/TIGER2010/VTD/2010/tl_2010_{fips}_vtd10.zip",
    "vtd/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/VTD/2020/tl_2020_{fips}_vtd20.zip",
    "place/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/PLACE/2010/tl_2020_{fips}_place10.zip",
    "place/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/PLACE/2020/tl_2020_{fips}_place20.zip",
    "cousub/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/COUSUB/2010/tl_2020_{fips}_cousub10.zip",
    "cousub/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/COUSUB/2020/tl_2020_{fips}_cousub20.zip",
    "aiannh/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/AIANNH/2010/tl_2020_{fips}_aiannh10.zip",
    "aiannh/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/AIANNH/2020/tl_2020_{fips}_aiannh20.zip",
}

COLUMN_CONFIG_PATH = Path(__file__).parent / "columns" / "pl_geo.yaml"


@click.command()
@click.option("--fips", help="State/territory FIPS code.", required=True)
@click.option("--level", type=click.Choice(LEVELS), required=True)
@click.option("--year", type=click.Choice(["2010", "2020"]), required=True)
@click.option("--namespace", required=True)
def load_geo(fips: str, level: str, year: str, namespace: str):
    """Imports base Census geographies.

    Preconditions:
        * A `Locality` aliased to `fips` exists.
        * `namespace` exists.
        * A `GeoLayer` with path `<level>/<year>` exists in the namespace.
    """
    db = CherryDB(namespace=namespace)
    loc = db.localities[fips]
    layer = db.geo_layers[f"/{level}/{year}"]

    with db.context(notes=""):
        pass


if __name__ == "__main__":
    config_logger()
    load_geo()
