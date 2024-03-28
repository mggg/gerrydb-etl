"""Imports localities for states, territories, and counties/county equivalents."""
import logging
import warnings
from collections import Counter

import click
import geopandas as gpd
import pandas as pd
import us
from gerrydb import GerryDB
from gerrydb.schemas import LocalityCreate
from gerrydb_etl import config_logger, download_dataframe_with_hash, pathify
from utm import from_latlon

log = logging.getLogger()

COUNTY_2010_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/tl_2010_us_county10.zip"
)
COUNTY_2020_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2020/COUNTY/tl_2020_us_county.zip"
)

NOTE_PREFIX = f"Loaded by ETL script {__file__} from the"
STATE_NOTES = f"{NOTE_PREFIX} `us` package (https://github.com/unitedstates/python-us)"
COUNTY_NOTES = (
    f"{NOTE_PREFIX} 2010 U.S. Census counties shapefile ({COUNTY_2010_URL}) "
    f"and the 2020 U.S. Census counties shapefile ({COUNTY_2020_URL})"
)

# For disambiguation.
# Most overrides are in Virginia, which has many independent cities that share
# names (as defined by the `NAME` column in the counties shapefile) with counties.
# see https://en.wikipedia.org/wiki/Independent_city_(United_States)
CANONICAL_PATH_OVERRIDES = {
    # Baltimore is an independent city.
    "24005": "maryland/baltimore-county",
    "24510": "maryland/city-of-baltimore",
    # St. Louis is an independent city.
    "29189": "missouri/st-louis-county",
    "29510": "missouri/city-of-st-louis",
    # Fairfax is an independent city.
    "51059": "virginia/fairfax-county",
    "51600": "virginia/city-of-fairfax",
    # Franklin is an independent city.
    "51067": "virginia/franklin-county",
    "51620": "virginia/city-of-franklin",
    # Roanoke is an independent city.
    "51161": "virginia/roanoke-county",
    "51770": "virginia/city-of-roanoke",
    # Richmond is an independent city (and state capital).
    "51159": "virginia/richmond-county",
    "51760": "virginia/city-of-richmond",
    # Bedford was an independent city from 1968 to 2013.
    "51019": "virginia/bedford-county",
    "51515": "virginia/city-of-bedford",
}
ALIAS_OVERRIDES = {
    "24005": ["md/baltimore-county"],
    "24510": ["md/city-of-baltimore", "maryland/baltimore-city", "md/baltimore-city"],
    "29189": ["mo/st-louis-county"],
    "29510": ["mo/city-of-st-louis", "missouri/st-louis-city", "mo/st-louis-city"],
    "51059": ["va/fairfax-county"],
    "51600": ["va/city-of-fairfax", "virginia/fairfax-city", "va/fairfax-city"],
    "51067": ["va/franklin-county"],
    "51620": ["va/city-of-franklin", "virginia/franklin-city", "va/franklin-city"],
    "51161": ["va/roanoke-county"],
    "51770": ["va/city-of-roanoke", "virginia/roanoke-city", "va/roanoke-city"],
    "51159": ["va/richmond-county"],
    "51760": ["va/city-of-richmond", "virginia/richmond-city", "va/richmond-city"],
    "51019": ["va/bedford-county"],
    "51515": ["va/city-of-bedford", "virginia/bedford-city", "va/bedford-city"],
}


def utm_of_point(point):
    """Returns the UTM zone of a lat-long point."""
    return from_latlon(point.y, point.x)[2]


def identify_utm_zone(df):
    """Identifies the modal UTM zone of a `GeoDataFrame` in lat-long coordinates."""
    # borrowed from gerrychain.graph.geo
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        utm_counts = Counter(utm_of_point(point) for point in df["geometry"].centroid)
    return utm_counts.most_common(1)[0][0]


def utm_zone_proj(zone: int) -> str:
    """Returns an EPSG identifier for a zone-appropriate UTM projection."""
    if 3 <= zone <= 20:
        # EPSG:26901 through EPSG:26920 are UTM projections appropriate
        # for the continental U.S., Hawaii, and Alaska.
        return f"epsg:269{str(zone).zfill(2)}"
    if zone == 2:
        # The only locality in UTM zone 2 covered by the decennial Census
        # is American Samoa, which is also the only Census locality in
        # the Southern Hemisphere.
        return "epsg:6636"  # NAD83(PA11) / UTM zone 2S
    if zone == 55:
        # Gaum and Northern Mariana Islands are in zone 55N.
        return "epsg:8693"  # NAD83(MA11) / UTM zone 55N
    raise ValueError("Zone not covered by the U.S. Census.")


@click.command()
def load_localities():
    """Imports localities for states, territories, and counties/county equivalents."""
    db = GerryDB()
    counties_gdf, counties_hash = download_dataframe_with_hash(COUNTY_2020_URL)

    # Cross-vintage compatibility: prefer 2020 data, but add legacy counties
    # that were eliminated between 2010 and 2020.
    counties_2010_gdf, counties_2010_hash = download_dataframe_with_hash(
        COUNTY_2010_URL
    )
    counties_2010_gdf = counties_2010_gdf.rename(
        columns={
            col: col[:-2] if col.endswith("10") else col
            for col in counties_2010_gdf.columns
        }
    )
    legacy_counties = set(counties_2010_gdf["GEOID"]) - set(counties_gdf["GEOID"])
    legacy_counties_gdf = counties_2010_gdf[
        counties_2010_gdf["GEOID"].isin(legacy_counties)
    ]
    counties_gdf = gpd.GeoDataFrame(
        pd.concat([counties_gdf, legacy_counties_gdf], ignore_index=True),
        crs=counties_gdf.crs,
    )

    state_like = us.STATES_AND_TERRITORIES + [us.states.lookup("DC")]
    with db.context(notes=STATE_NOTES) as ctx:
        log.info("Creating root locality (us)...")
        ctx.localities.create(
            canonical_path="us",
            name="United States of America",
        )

        state_like_locs = []
        for state in state_like:
            log.info("Creating locality for state/territory %s...", state.name)
            state_gdf = counties_gdf[counties_gdf["STATEFP"] == state.fips]
            zone = identify_utm_zone(state_gdf)
            state_like_locs.append(
                LocalityCreate(
                    canonical_path=pathify(state.name),
                    parent_path="us",
                    name=state.name,
                    aliases=[state.fips, state.abbr.lower()],
                    default_proj=utm_zone_proj(zone),
                )
            )

        log.info(
            "Pushing localities for %d states/territories...", len(state_like_locs)
        )
        ctx.localities.create_bulk(state_like_locs)

    state_fips_to_name = {state.fips: state.name for state in state_like}
    state_fips_to_abbr = {state.fips: state.abbr for state in state_like}

    counties_gdf = counties_gdf[counties_gdf.STATEFP != "11"].copy()  # drop DC
    counties_gdf["state_name"] = counties_gdf["STATEFP"].map(state_fips_to_name)
    counties_gdf["state_abbr"] = counties_gdf["STATEFP"].map(state_fips_to_abbr)
    counties_gdf["full_name"] = (
        counties_gdf["NAMELSAD"] + ", " + counties_gdf["state_name"]
    )
    counties_gdf = counties_gdf.sort_values(by=["GEOID"])

    with db.context(
        notes=(
            COUNTY_NOTES
            + f" (2010 SHA256: {counties_2010_hash.hexdigest()}, "
            + f"2020 SHA256: {counties_hash.hexdigest()})"
        )
    ) as ctx:
        county_locs = []
        for row in counties_gdf.itertuples():
            log.info("Creating locality for %s...", row.full_name)
            zone = utm_of_point(row.geometry.centroid)
            canonical_path = CANONICAL_PATH_OVERRIDES.get(
                row.GEOID, f"{pathify(row.state_name)}/{pathify(row.NAME)}"
            )
            aliases = ALIAS_OVERRIDES.get(
                row.GEOID, [f"{pathify(row.state_abbr)}/{pathify(row.NAME)}"]
            ) + [row.GEOID]

            county_locs.append(
                LocalityCreate(
                    canonical_path=canonical_path,
                    parent_path=pathify(row.state_name),
                    name=row.full_name,
                    aliases=aliases,
                    default_proj=utm_zone_proj(zone),
                )
            )

        log.info(
            "Pushing localities for %d counties/county equivalents...", len(county_locs)
        )
        ctx.localities.create_bulk(county_locs)


if __name__ == "__main__":
    config_logger(log)
    load_localities()
