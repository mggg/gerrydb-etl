"""Creates columns for Census PL 94-171 tables P1 through P4."""

import logging
from typing import Optional

import click
import httpx
from gerrydb import GerryDB
from gerrydb_etl import config_logger
from gerrydb.exceptions import ResultError

log = logging.getLogger()

SOURCE_URL = "https://api.census.gov/data/{year}/dec/pl"
COL_LABELS = {
    "Asian": "asian",
    "White": "white",
    "Black": "black",
    "Some Other Race": "other",
    "Black or African American": "black",
    "Native Hawaiian and Other Pacific Islander": "nhpi",
    "American Indian and Alaska Native": "amin",
    "Total": "total",
    "Population of one race": "one_race",
    "Population of two or more races": "two_or_more_races",
    "Population of two races": "two_races",
    "Population of three races": "three_races",
    "Population of four races": "four_races",
    "Population of five races": "five_races",
    "Population of six races": "six_races",
}
TABLES = {
    "P1": "Race",
    "P2": "Hispanic or Latino, and Not Hispanic or Latino by Race",
    "P3": "Race for the Population 18 Years and Over",
    "P4": (
        "Hispanic or Latino, and Not Hispanic or Latino by Race "
        "for the Population 18 Years and Over"
    ),
}

# pairs are (table, Hispanic/Latino flag)
COL_DESCRIPTIONS = {
    ("P1", None): "population by race",
    ("P2", True): "Hispanic population by race",
    ("P2", False): "non-Hispanic or Latino population by race",
    ("P3", None): "voting-age population by race",
    ("P4", True): "Hispanic or Latino voting-age population by race",
    ("P4", False): "non-Hispanic or Latino voting-age population by race",
}

# Some columns are shared between tables; we avoid importing these twice.
REDUNDANT_COLUMN_TO_CANONICAL_COLUMN = {
    # total population
    "P2_001N": "P1_001N",  # 2020
    "P002001": "P001001",  # 2010
    # total VAP
    "P4_001N": "P3_001N",  # 2020
    "P004001": "P003001",  # 2010
}
CANONICAL_COLUMN_TO_REDUNDANT_COLUMN = {
    v: k for k, v in REDUNDANT_COLUMN_TO_CANONICAL_COLUMN.items()
}


def parse_labels(variables: dict) -> dict[str, tuple[str, str, Optional[bool]]]:
    """Filters and parses longform Census P1-P4 column labels."""
    mapped_labels = {}
    for key, meta in variables.items():
        if meta["label"] == "Total" or meta.get("predicateType") == "int":
            label = meta["label"].replace(":", "").split("!!")[-1]
            label = label.replace("alone", "").strip()
            # Correct for bug in 2010 Census P004059 label.
            label = label.replace(", Some Other Race", "; Some Other Race")
            label_parts = label.split("; ")

            if "Hispanic or Latino" in meta["label"]:
                hispanic = not "Not Hispanic or Latino" in meta["label"]
            else:
                hispanic = None

            mapped_labels[key] = (
                label,
                "_".join(COL_LABELS.get(part, part) for part in label_parts),
                hispanic,
            )
    return mapped_labels


def column_aliases(name: str) -> Optional[list[str]]:
    """Standardizes equivalent Census column names across formats/vintages.

    The 2010 PL 94-171 release uses the column name format Pxxxyyy (e.g. `P001001`),
    and the format Pxxxyyyy (e.g. `P0010001`) also crops up in some usages.

    The 2020 PL 94-171 release uses the column name format Px_yyyN (e.g. `P1_001N`);
    there are also columns with name format Px_yyyNA that contain annotations, but
    we do not currently import these.

    Returns:
        All three column name formats for `name`, assuming the format of `name`
        can be identified.
    """
    if name.startswith("P") and name[2] == "_" and len(name) == 7:
        table_id = name[1]
        col_id = name[3:6]
        return [name, f"P00{table_id}{col_id}", f"P00{table_id}0{col_id}"]
    if name.startswith("P00") and len(name) == 7:
        table_id = name[3]
        col_id = name[4:]
        return [f"P{table_id}_{col_id}N", name, f"P00{table_id}0{col_id}"]
    if name.startswith("P00") and len(name) == 8:
        table_id = name[3]
        col_id = name[5:]
        return [f"P{table_id}_{col_id}N", f"P00{table_id}{col_id}", name]
    return None


@click.command()
@click.option("--namespace", required=True)
@click.option("--year", required=True)
def create_columns(namespace: str, year: str):
    """Creates columns for Census tables P1 through P4."""
    base_url = SOURCE_URL.format(year=year)
    table_urls = {table: f"{base_url}/groups/{table}/" for table in TABLES}
    table_variables = {}
    for table, table_url in table_urls.items():
        response = httpx.get(url=table_url, headers={"accept": "application/json"})
        response.raise_for_status()
        table_variables[table] = response.json()["variables"]

    db = GerryDB(namespace=namespace)

    # Load columns by table.
    #
    # This is complicated by some overlap between tables:
    # the tables that break population down into Hispanic/non-Hispanic groups
    # (Table P2 and Table P4) include *overall* total population columns that
    # also show up under a different name in Table P1 and P3, respectively.
    #
    # We somewhat arbitrarily choose the columns in P1 and P3; this choice is
    # enforced by the ordering of `TABLES`.
    redundant_columns = {}
    table_variables_parsed = {
        table: parse_labels(variables) for table, variables in table_variables.items()
    }

    for table, concept in TABLES.items():
        with db.context(
            notes=(
                f"ETL script {__file__}: creating columns for {year} "
                f"U.S. Census P.L. 94-171 Table {table} ({concept})"
            )
        ) as ctx:
            variables = table_variables_parsed[table]
            table_cols = []

            for census_name, (
                demographic,
                canonical_name,
                col_is_hispanic,
            ) in variables.items():

                # if column is redundant
                if census_name in REDUNDANT_COLUMN_TO_CANONICAL_COLUMN:
                    log.info("Skipping column %s (redundant)...", census_name)
                    # use the column that corresponds to redundant
                    table_cols.append(redundant_columns[census_name])
                    continue

                if col_is_hispanic is None:
                    prefix = ""
                else:
                    prefix = "hispanic_" if col_is_hispanic else "non_hispanic_"
                suffix = "_vap" if table in ("P3", "P4") else "_pop"

                if canonical_name.endswith("Hispanic or Latino"):
                    # Avoid column names like `hispanic_hispanic_pop`.
                    col_name = prefix + suffix[1:]
                else:
                    col_name = prefix + canonical_name + suffix

                col_name = col_name.replace(" ", "_")
                log.info(
                    "Creating Table %s column %s (from %s) in namespace %s...",
                    table,
                    col_name,
                    census_name,
                    namespace,
                )
                col_description = COL_DESCRIPTIONS[(table, col_is_hispanic)]
                aliases = column_aliases(census_name)
                if census_name in CANONICAL_COLUMN_TO_REDUNDANT_COLUMN:
                    redundant_name = CANONICAL_COLUMN_TO_REDUNDANT_COLUMN[census_name]
                    log.info(
                        "Adding additional aliases from redundant column %s...",
                        redundant_name,
                    )
                    aliases += column_aliases(redundant_name)
                else:
                    redundant_name = None

                try:
                    # try to create the column
                    log.debug(f"making the column {col_name.lower()}")
                    log.debug(
                        f"\tdescription: {year} U.S. Census {col_description}: {demographic}"
                    )
                    col = ctx.columns.create(
                        col_name.lower(),
                        aliases=[alias.lower() for alias in aliases],
                        column_kind="count",
                        column_type="int",
                        description=(
                            f"{year} U.S. Census {col_description}: " + demographic
                        ),
                        source_url=table_urls[table],
                    )
                    table_cols.append(col)
                    if redundant_name is not None:
                        redundant_columns[redundant_name] = col

                except ResultError as e:

                    # if the column already exists, get the column from the database
                    if "Failed to create column" in e.args[0]:
                        # get col from database
                        col = ctx.columns.get(col_name.lower())

                        log.info(
                            f"Failed to create {col_name} column, already in namespace {namespace}"
                        )
                        log.info("Using existing column")
                        table_cols.append(col)
                        if redundant_name is not None:
                            redundant_columns[redundant_name] = col
                    else:
                        raise e

            log.info("Creating column set for Table %s...", table)
            try:
                ctx.column_sets.create(
                    path=table.lower(),
                    columns=table_cols,
                    description=f"{year} U.S. Census P.L. 94-171 Table {table}",
                )
            except ResultError as e:
                if "Failed to create column set" in e.args[0]:
                    log.info(
                        f"Failed to create {table.lower()} column set, already exists"
                    )
                else:
                    raise e


if __name__ == "__main__":
    config_logger(log)
    create_columns()
