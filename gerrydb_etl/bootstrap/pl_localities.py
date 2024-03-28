"""Imports localities for states, territories, and counties/county equivalents."""
import logging
import warnings
from collections import Counter

import click
import geopandas as gpd
import pandas as pd
import us # package with tons of state metadata

from gerrydb import GerryDB
from gerrydb.exceptions import ResultError
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

# __file__ is the absolute path of the pl_localities.py script
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

# utm is Universal Transverse Mercator, it's one of 60 zones used for projections
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

# EPSG European Petroleum Survey Group codes, used for identifying projection systems
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


# click is a library designed to create CLI (command line interface) programs
# decorator, but nothing seems to be used here 
@click.command()
@click.option("--suppress_existence", 
              is_flag = True, 
              flag_value  = True,
              help="Handle 'path already exists' errors.")
def load_localities(suppress_existence: bool):
    """Imports localities for states, territories, and counties/county equivalents."""

    # creates a gerrydb instance, which sets up communication between docker container DB and uvicorn web server
    db = GerryDB()

    # Returns a (Geo)DataFrame and a file hash from a downloaded file
    counties_gdf, counties_hash = download_dataframe_with_hash(COUNTY_2020_URL)

    # Cross-vintage compatibility: prefer 2020 data, but add legacy counties
    # that were eliminated between 2010 and 2020.
    # Returns a (Geo)DataFrame and a file hash from a downloaded file
    counties_2010_gdf, counties_2010_hash = download_dataframe_with_hash(
        COUNTY_2010_URL
    )

    # remove 10 from the end of each column name
    counties_2010_gdf = counties_2010_gdf.rename(
        columns={
            col: col[:-2] if col.endswith("10") else col
            for col in counties_2010_gdf.columns
        }
    )

    # counties removed between 2010 and 2020 census
    legacy_counties = set(counties_2010_gdf["GEOID"]) - set(counties_gdf["GEOID"])
    legacy_counties_gdf = counties_2010_gdf[
        counties_2010_gdf["GEOID"].isin(legacy_counties)
    ]

    # create new geodataframe that includes 2020 and legacy counties
    # TODO concat is being deprecated
    counties_gdf = gpd.GeoDataFrame(
        pd.concat([counties_gdf, legacy_counties_gdf], ignore_index=True),
        crs=counties_gdf.crs,
    )

    # list of states, territories, and DC
    state_like = us.STATES_AND_TERRITORIES + [us.states.lookup("DC")]

    # add state, territory, DC, and USA localities
    # ctx is a WriteContext object, which stores all sorts of meta data about our transaction
    with db.context(notes=STATE_NOTES) as ctx:
        log.info("Creating root locality (us)...")

        # ctx.localities returns the LocalityRepo object associated to the GerryDB instance
        # create method creates a locality, in this case the US
        # will raise ResultError if path already exists
        try:
            ctx.localities.create(
                canonical_path="us", # short identifier
                name="United States of America", # full name 
            )
        except ResultError as e:
            if suppress_existence and "Failed to create canonical path to new location(s)." in e.args[0]:
                print("U.S. Path already exists.")
                pass
            else:
                raise e

        state_like_locs = []

        # for each state, territory, and DC
        for state in state_like:
            log.info("Creating locality for state/territory %s...", state.name)

            # access all counties in state
            state_gdf = counties_gdf[counties_gdf["STATEFP"] == state.fips] 

            # identifies modal utm zone (locality could cross several)
            zone = identify_utm_zone(state_gdf)

            # LocalityCreate stores info about the locality (but notably not the underlying geography!)
            state_like_locs.append(
                LocalityCreate(
                    canonical_path=pathify(state.name), #GerryPath constrained string
                    parent_path="us", # GerryPath constrained string, probably denotes hierarchy
                    name=state.name, #name of locality
                    aliases=[state.fips, state.abbr.lower()], # aliases is a list of GerryPath constrained strings
                    default_proj=utm_zone_proj(zone), # default geoegraphic projection
                )
            )

        log.info(
            "Pushing localities for %d states/territories...", len(state_like_locs)
        )
        # create bulk creates localities in bulk from a list of LocalityCreate objects
        # TODO unclear on how this actually ends up in the database

        ctx.localities.create_bulk(state_like_locs)
        


    # us module has these built in under mapping method, but parker
    # is being intentional about DC
    state_fips_to_name = {state.fips: state.name for state in state_like}
    state_fips_to_abbr = {state.fips: state.abbr for state in state_like}

    # create a version of the dataframe without DC
    counties_gdf = counties_gdf[counties_gdf.STATEFP != "11"].copy()  # drop DC

    # add new columns
    counties_gdf["state_name"] = counties_gdf["STATEFP"].map(state_fips_to_name)
    counties_gdf["state_abbr"] = counties_gdf["STATEFP"].map(state_fips_to_abbr)
    counties_gdf["full_name"] = (
        counties_gdf["NAMELSAD"] + ", " + counties_gdf["state_name"]
    )
    counties_gdf = counties_gdf.sort_values(by=["GEOID"])

    # create context object to store meta data of transaction
    # add counties
    # TODO what are these hash things being stored?
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

            # if the row is in the override dict, use override value
            # else use f"{pathify(row.state_name)}/{pathify(row.NAME)}"
            canonical_path = CANONICAL_PATH_OVERRIDES.get(
                row.GEOID, f"{pathify(row.state_name)}/{pathify(row.NAME)}"
            )
            # if the row is in the override dict, use override value
            # else use f"{pathify(row.state_abbr)}/{pathify(row.NAME)}"

            # adding the geoid and this path as aliases
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
        


# meant to say this program is meant to be run, not part of library/package
if __name__ == "__main__":
    config_logger(log) # Configures a logger to write to `stderr`, from gerrydb_etl.__init__
    load_localities()
