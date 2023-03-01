"""Imports base Census geographies."""
import logging

import click
from cherrydb import CherryDB

from cherrydb_etl import config_logger

log = logging.getLogger()

NOTES = """Loaded by"""


@click.command()
def load_geo():
    """Imports base Census geographies."""
    db = CherryDB()


if __name__ == "__main__":
    config_logger()
    load_geo()
