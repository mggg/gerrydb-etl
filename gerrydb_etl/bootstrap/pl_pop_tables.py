"""Loads Census PL 94-171 tables P1 through P4 from the Census API."""
import logging
import os

import click
import httpx
import pandas as pd
from gerrydb import GerryDB
from gerrydb_etl import config_logger
from gerrydb_etl.bootstrap.pl_config import (AUXILIARY_LEVELS, LEVELS,
                                             MISSING_DATASETS, MissingDataset)

try:
    from gerrydb_etl.db import DirectTransactionContext
    from gerrydb_meta import crud, models
    from sqlalchemy import select
except ImportError:
    crud = None

log = logging.getLogger()

TABLES = ("P1", "P2", "P3", "P4")
SOURCE_URL = "https://api.census.gov/data/{year}/dec/pl"
CENTRAL_SPINE_LEVELS = (
    "block",
    "bg",
    "tract",
    "county",
    "state",
)
# Levels auxiliary to central spine.
AUXILIARY_LEVELS = (
    "vtd",
    "place",
    "cousub",
    "aiannh",  # American Indian/Alaska Native/Native Hawaiian Areas
)
LEVELS = CENTRAL_SPINE_LEVELS + AUXILIARY_LEVELS


@click.command()
@click.option("--namespace", required=True)
@click.option("--year", required=True)
@click.option("--table", required=True, type=click.Choice(TABLES))
@click.option("--level", required=True, type=click.Choice(LEVELS))
@click.option("--fips", help="State/territory FIPS code.")
def load_tables(namespace: str, year: str, table: str, level: str, fips: str):
    """
    Loads Census PL 94-171 tables P1 through P4 from the Census API.

    https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf
    https://api.census.gov/data.html
    
    """
    if MissingDataset(fips=fips, level=level, year=year) in MISSING_DATASETS:
        log.warning("Dataset not published by Census. Nothing to do.")
        exit()

    if fips is None:
        raise ValueError(f'Level "{level}" requires a state FIPS code.')

    if os.getenv("GERRYDB_BULK_IMPORT") and crud is None:
        raise RuntimeError("gerrydb_meta must be available in bulk import mode.")

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
        query = {"for": f"state:{fips}"}
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
        query = {"for": f"american indian area/alaska native area/hawaiian home land (or part):*", "in":f"state:{fips}"}
        id_cols = ("american indian area/alaska native area/hawaiian home land (or part)",)
        
    else:
        raise ValueError("Unknown level.")

    db = GerryDB(namespace=namespace)
    table_cols = db.column_sets[table.lower()]
    col_aliases = {}
    for col in table_cols.columns:
        for alias in col.aliases:
            col_aliases[alias] = col

    # params are Query parameters to include in the URL, 
    # as a string, dictionary, or sequence of two-tuples.
    response = httpx.get(
        url=SOURCE_URL.format(year=year), params={**base_params, **query}, timeout=300
    )
    response.raise_for_status()

    rows = response.json()
    table_df = pd.DataFrame.from_records(rows[1:], columns=rows[0])
    table_df["id"] = table_df[list(id_cols)].agg("".join, axis=1)

    if level in AUXILIARY_LEVELS:
        # since aiannh geographies cross state lines, the census subidivides the polygon but
        # uses the same geoid, we add the fips code to make the geoid unique
        table_df["id"] = f"{level}:" + table_df["id"]+ f":fips{fips}" if level == "aiannh" else f"{level}:" + table_df["id"]

    table_df = table_df.rename(columns={col: col.lower() for col in table_df.columns})
    table_df = table_df.set_index("id")

    table_cols = {
        alias: col for alias, col in col_aliases.items() if alias in table_df.columns
    }
    for col in table_cols:
        table_df[col] = table_df[col].astype(int)

    import_notes = (
        f"ETL script {__file__}: loading data for {year} "
        f"U.S. Census P.L. 94-171 Table {table}"
    )

    
    if os.getenv("GERRYDB_BULK_IMPORT"):
        log.info(
            "Importing column data via bulk import mode (direct database access)..."
        )
        with DirectTransactionContext(notes=import_notes) as ctx:
            namespace_obj = crud.namespace.get(db=ctx.db, path=namespace)
            assert namespace_obj is not None

            geographies = crud.geography.get_bulk(
                db=ctx.db,
                namespaced_paths=[(namespace, idx) for idx in table_df.index],
            )
            if len(geographies) < len(table_df):
                raise ValueError(
                    f"Cannot perform bulk import (expected {len(table_df)} "
                    f"geographies, found {len(geographies)})."
                )

            raw_cols = (
                ctx.db.query(models.DataColumn)
                .filter(
                    models.DataColumn.col_id.in_(
                        select(models.ColumnRef.col_id).filter(
                            models.ColumnRef.path.in_(
                                col.path for col in table_cols.values()
                            ),
                            models.ColumnRef.namespace_id == namespace_obj.namespace_id,
                        )
                    )
                )
                .all()
            )
            cols_by_canonical_path = {col.canonical_ref.path: col for col in raw_cols}
            cols_by_alias = {
                alias: cols_by_canonical_path[col.canonical_path]
                for alias, col in table_cols.items()
            }
            geos_by_path = {geo.path: geo for geo in geographies}
            ctx.load_column_values(cols=cols_by_alias, geos=geos_by_path, df=table_df)
    else:
        log.info("Importing column data via API...")
        with db.context(notes=import_notes) as ctx:
            ctx.load_dataframe(table_df, table_cols)


if __name__ == "__main__":
    config_logger(log)
    load_tables()
