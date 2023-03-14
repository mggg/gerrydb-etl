"""Loads Census PL 94-171 tables P1 through P4 from the Census API."""
import logging
import os

import click
import httpx
import pandas as pd
from cherrydb import CherryDB

from cherrydb_etl import config_logger
from cherrydb_etl.db import DirectTransactionContext

log = logging.getLogger()

TABLES = ("P1", "P2", "P3", "P4")
SOURCE_URL = "https://api.census.gov/data/{year}/dec/pl"
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


@click.command()
@click.option("--namespace", required=True)
@click.option("--year", required=True)
@click.option("--table", required=True, type=click.Choice(TABLES))
@click.option("--level", required=True, type=click.Choice(LEVELS))
@click.option("--fips", help="State/territory FIPS code.")
def load_tables(namespace: str, year: str, table: str, level: str, fips: str):
    """Loads Census PL 94-171 tables P1 through P4 from the Census API."""
    if level in ("state", "aiannh") and fips is not None:
        raise ValueError(f'Level "{level}" is national (no state FIPS code used).')
    elif fips is None:
        raise ValueError(f'Level "{level}" requires a state FIPS code.')

    base_params = {"get": f"group({table})"}
    api_key = os.getenv("CENSUS_API_KEY")
    if api_key is not None:
        base_params["key"] = api_key

    if level == "block":
        query = {"in": f"state:{fips} county:*", "for": "block:*"}
        id_cols = ("state", "county", "tract", "block")
    elif level == "bg":
        query = {"in": f"state:{fips} county:*", "for": "block group:*"}
        id_cols = ("state", "county", "tract", "block group")
    elif level == "tract":
        query = {"in": f"state:{fips}", "for": "tract:*"}
        id_cols = ("state", "county", "tract")
    elif level == "county":
        query = {"in": f"state:{fips}", "for": "county:*"}
        id_cols = ("state", "county")
    elif level == "state":
        query = {"for": "state:*"}
        id_cols = ("state",)
    elif level == "vtd":
        query = {"in": f"state:{fips}", "for": "voting district:*"}
        id_cols = ("state", "county", "voting district")
    elif level == "place":
        query = {"in": f"state:{fips}", "for": "place:*"}
        id_cols = ("state", "place")
    elif level == "cousub":
        query = {"in": f"state:{fips}", "for": "county subdivision:*"}
        id_cols = ("state", "county", "county subdivision")
    elif level == "aiannh":
        query = {"for": "american indian area/alaska native area/hawaiian home land:*"}
        id_cols = ("american indian area/alaska native area/hawaiian home land",)
    else:
        raise ValueError("Unknown level.")

    db = CherryDB(namespace=namespace)
    table_cols = db.column_sets[table.lower()]
    col_aliases = {}
    for col in table_cols.columns:
        for alias in col.aliases:
            col_aliases[alias] = col

    response = httpx.get(
        url=SOURCE_URL.format(year=year), params={**base_params, **query}
    )
    response.raise_for_status()

    rows = response.json()
    table_df = pd.DataFrame.from_records(rows[1:], columns=rows[0])
    table_df["id"] = table_df[list(id_cols)].agg("".join, axis=1)
    table_df = table_df.rename(columns={col: col.lower() for col in table_df.columns})
    table_df = table_df.set_index("id")

    table_cols = {
        alias: col for alias, col in col_aliases.items() if alias in table_df.columns
    }
    for col in table_cols:
        table_df[col] = table_df[col].astype(int)

    with DirectTransactionContext(
        notes=(
            f"ETL script {__file__}: loading data for {year} "
            f"U.S. Census P.L. 94-171 Table {table}"
        )
    ) as ctx:
        ctx.load_dataframe(table_df, table_cols)


if __name__ == "__main__":
    config_logger(log)
    load_tables()
