"""Imports localities for states, territories, and counties/county equivalents."""
import logging

import click
import us
from cherrydb import CherryDB

from cherrydb_etl import config_logger, download_dataframe_with_hash, pathify

log = logging.getLogger()

COUNTIES_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2020/COUNTY/tl_2020_us_county.zip"
)

NOTE_PREFIX = "Loaded by {__name__} ETL script from the"
STATE_NOTES = f"{NOTE_PREFIX} `us` package (https://github.com/unitedstates/python-us)"
COUNTY_NOTES = f"{NOTE_PREFIX} 2020 U.S. Census counties shapefile ({COUNTIES_URL})"


@click.command()
def load_localities():
    """Imports localities for states, territories, and counties/county equivalents."""
    db = CherryDB()
    counties_gdf, counties_hash = download_dataframe_with_hash(COUNTIES_URL)

    with db.context(notes=STATE_NOTES) as ctx:
        log.info("Creating root locality (us)...")
        ctx.localities.create(
            canonical_path="us",
            name="United States of America",
        )
        for state in us.STATES_AND_TERRITORIES:
            log.info("Creating locality for state/territory %s...", state.name)
            ctx.localities.create(
                canonical_path=pathify(state.name),
                parent_path="us",
                name=state.name,
                aliases=[state.fips, state.abbr.lower()],
            )

    state_fips_to_name = {state.fips: state.name for state in us.STATES_AND_TERRITORIES}
    state_fips_to_abbr = {state.fips: state.abbr for state in us.STATES_AND_TERRITORIES}
    counties_gdf["state_name"] = counties_gdf["STATEFP"].map(state_fips_to_name)
    counties_gdf["state_abbr"] = counties_gdf["STATEFP"].map(state_fips_to_abbr)
    counties_gdf["full_name"] = (
        counties_gdf["NAMELSAD"] + ", " + counties_gdf["state_name"]
    )

    with db.context(
        notes=COUNTY_NOTES + f" (SHA256: {counties_hash.hexdigest()})"
    ) as ctx:
        for row in counties_gdf.itertuples():
            log.info("Creating locality for %s...", row.full_name)
            ctx.localities.create(
                canonical_path=f"{pathify(row.state_name)}/{pathify(row.NAME)}",
                parent_path=pathify(row.state_name),
                name=row.full_name,
                aliases=[
                    row.GEOID,
                    f"{pathify(row.state_abbr)}/{pathify(row.NAME)}",
                ],
            )


if __name__ == "__main__":
    config_logger(log)
    load_localities()

# Ideas:
#   * self-test framework
#   * locality bulk loading (simple async, kind of a hack)
#   * @if_not_exists / idempotency at the ETL framework level?
