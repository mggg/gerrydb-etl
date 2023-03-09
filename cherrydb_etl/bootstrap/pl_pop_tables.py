"""Loads Census PL 94-171 tables P1 through P4 from the Census API."""
import logging
from typing import Optional

import click
import httpx
from cherrydb import CherryDB

from cherrydb_etl import config_logger

log = logging.getLogger()

SOURCE_URL = "https://api.census.gov/data/{year}/dec/pl"


@click.command()
@click.option("--namespace", required=True)
@click.option("--year", required=True)
@click.option("--table", required=True, type=click.Choice(["P1", "P2", "P3", "P4"]))
def create_columns(namespace: str, year: str, table: str):
    """Creates columns for a Census tables P1 through P4."""
    base_url = SOURCE_URL.format(year=year)
    table_urls = {table: f"{base_url}/groups/{table}/" for table in TABLES}

    db = CherryDB(namespace=namespace)

    with db.context(
        notes=(
            f"ETL script {__file__}: loading data for {year} "
            f"U.S. Census P.L. 94-171 Table {table}"
        )
    ) as ctx:
        pass


if __name__ == "__main__":
    config_logger(log)
    create_columns()
