"""Imports localities for states, territories, and counties/county equivalents."""
import logging
import warnings
from collections import Counter

import click
import us
from cherrydb import CherryDB
from utm import from_latlon

from cherrydb_etl import config_logger, download_dataframe_with_hash, pathify

log = logging.getLogger()

COUNTIES_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2020/COUNTY/tl_2020_us_county.zip"
)

NOTE_PREFIX = "Loaded by {__name__} ETL script from the"
STATE_NOTES = f"{NOTE_PREFIX} `us` package (https://github.com/unitedstates/python-us)"
COUNTY_NOTES = f"{NOTE_PREFIX} 2020 U.S. Census counties shapefile ({COUNTIES_URL})"

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


def utm_zone_projtext(zone: int) -> str:
    """Returns the projtext for EPSG:269XX, where XX is a UTM zone code."""
    assert 1 <= zone <= 60
    return f"+proj=utm +zone={zone} +datum=NAD83 +units=m +no_defs"


@click.command()
def load_localities():
    """Imports localities for states, territories, and counties/county equivalents."""
    db = CherryDB()
    counties_gdf, counties_hash = download_dataframe_with_hash(COUNTIES_URL)

    state_like = us.STATES_AND_TERRITORIES + [us.states.lookup("DC")]
    with db.context(notes=STATE_NOTES) as ctx:
        log.info("Creating root locality (us)...")
        ctx.localities.create(
            canonical_path="us",
            name="United States of America",
        )
        for state in state_like:
            log.info("Creating locality for state/territory %s...", state.name)
            state_gdf = counties_gdf[counties_gdf["STATEFP"] == state.fips]
            zone = identify_utm_zone(state_gdf)
            ctx.localities.create(
                canonical_path=pathify(state.name),
                parent_path="us",
                name=state.name,
                aliases=[state.fips, state.abbr.lower()],
                default_proj=utm_zone_projtext(zone),
            )

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
        notes=COUNTY_NOTES + f" (SHA256: {counties_hash.hexdigest()})"
    ) as ctx:
        for row in counties_gdf.itertuples():
            log.info("Creating locality for %s...", row.full_name)
            zone = utm_of_point(row.geometry.centroid)
            canonical_path = CANONICAL_PATH_OVERRIDES.get(
                row.GEOID, f"{pathify(row.state_name)}/{pathify(row.NAME)}"
            )
            aliases = ALIAS_OVERRIDES.get(
                row.GEOID, [f"{pathify(row.state_abbr)}/{pathify(row.NAME)}"]
            ) + [row.GEOID]

            ctx.localities.create(
                canonical_path=canonical_path,
                parent_path=pathify(row.state_name),
                name=row.full_name,
                aliases=aliases,
                default_proj=utm_zone_projtext(zone),
            )


if __name__ == "__main__":
    config_logger(log)
    load_localities()

# Ideas:
#   * self-test framework
#   * locality bulk loading (simple async, kind of a hack)
#   * @if_not_exists / idempotency at the ETL framework level?
