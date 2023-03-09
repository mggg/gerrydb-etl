"""Imports base Census geographies."""
import logging
from pathlib import Path

import click
import yaml
from cherrydb import CherryDB
from jinja2 import Template
from shapely import Point

from cherrydb_etl import TabularConfig, config_logger, download_dataframe_with_hash

log = logging.getLogger()


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
SOURCE_URL = "https://www2.census.gov/geo/tiger/TIGER2020PL/"
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
    "vtd/2010": "https://www2.census.gov/geo/tiger/TIGER2010/VTD/2012/tl_2012_{fips}_vtd10.zip",
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
    root_loc = db.localities[fips]
    layer = db.geo_layers[level]

    with open(COLUMN_CONFIG_PATH) as config_fp:
        config_template = Template(config_fp.read())
    rendered_config = config_template.render(yr=year[2:], year=year)
    config = TabularConfig(**yaml.safe_load(rendered_config))

    layer_url = LAYER_URLS[f"{level}/{year}"].format(fips=fips)
    index_col = "GEOID" + year[2:]
    county_col = "COUNTYFP" + year[2:]
    layer_gdf, layer_hash = download_dataframe_with_hash(
        url=layer_url,
        dtypes=config.source_dtypes(),
    )
    geos_by_county = (
        dict(layer_gdf.groupby(county_col)[index_col].apply(list))
        if county_col in layer_gdf.columns
        else {}
    )
    layer_gdf = layer_gdf.set_index(index_col)

    columns = {
        col.source: db.columns[col.target]
        for col in config.columns
        if col.source in layer_gdf.columns
    }

    internal_latitudes = layer_gdf[f"INTPTLAT{year[2:]}"].apply(float)
    internal_longitudes = layer_gdf[f"INTPTLON{year[2:]}"].apply(float)
    layer_gdf["internal_point"] = [
        Point(long, lat) for long, lat in zip(internal_longitudes, internal_latitudes)
    ]

    with db.context(
        notes=(
            f"Loaded by ETL script {__name__} from {year} U.S. Census {level} "
            f"shapefile {layer_url} (SHA256: {layer_hash.hexdigest()})"
        )
    ) as ctx:
        ctx.load_dataframe(
            df=layer_gdf,
            columns=columns,
            create_geo=True,
            locality=root_loc,
            layer=layer,
        )
        for county_fips, county_geos in geos_by_county.items():
            full_fips = fips + county_fips
            log.info("Mapping units for county (equivalent) %s...", full_fips)
            ctx.geo_layers.map_locality(
                layer=layer, locality=full_fips, geographies=county_geos
            )


if __name__ == "__main__":
    config_logger(log)
    load_geo()
