# GerryDB ETL

This repository contains scripts used for bulk loading of data from the U.S. Census and other key sources into GerryDB.

## Rough bootstrapping guide

* Create cloud resources: PostgreSQL + container deployment
    * Our deployment: Cloud Run (w/ Cloud Build) + Cloud SQL
    * see https://cloud.google.com/sql/docs/postgres/connect-run
* Create database (see cloud-sql-proxy)
    CREATE DATABASE gerrydb; CREATE EXTENSION postgis;
* Optional: create ephemeral VM for imports
    Install pyenv (https://bgasparotto.com/install-pyenv-ubuntu-debian) + Python 3.11, gh (optional), git
    Clone repos
    pip install poetry, poetry install
* Grab SQL credentials (grant VM access or use cloud-sql-proxy, etc.)
* Create schema and superuser API key
    python nuke.py
* Create ~/.gerrydb/config
* Run ETL scripts 
    pl_init.sh (~5 min)
    pl_geo.sh

