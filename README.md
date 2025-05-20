# GerryDB ETL

This repository contains scripts used for bulk loading of data from the U.S. Census and other key sources into GerryDB.

## Rough bootstrapping guide

* Create cloud resources: PostgreSQL + container deployment
    * Our deployment: Cloud Run (w/ Cloud Build) + Cloud SQL
    * see https://cloud.google.com/sql/docs/postgres/connect-run
* Create database (see [`cloud-sql-proxy`](https://cloud.google.com/sql/docs/postgres/sql-proxy) to connect)
    ```sql
    CREATE DATABASE gerrydb; CREATE EXTENSION postgis;
    ```
* Optional: create ephemeral VM for imports
    * Install pyenv (https://bgasparotto.com/install-pyenv-ubuntu-debian) + Python 3.11, gh (optional), git
    * Clone repos (`gerrydb-client-py`, `gerrydb-etl`)
    * Run `pip install poetry` and `poetry install`
* Grab SQL credentials (grant ephemeral VM direct network access or use `cloud-sql-proxy`)
* Create schema and superuser API key (use `init.py` in `gerrydb-meta`)
* Add API credentials to `~/.gerrydb/config`
* Run ETL scripts 
    1. `gerrydb_etl/bootstrap/pl_init.sh` (~5 min)
    2. `gerrydb_etl/bootstrap/pl_geo_states.sh` (loads all state level geography)
    3. `gerrydb_etl/bootstrap/pl_geo_aiannh.sh` (loads all aiannh level geography)
    4. `gerrydb_etl/bootstrap/pl_pop_aiannh.sh` (loads all aiannh area populations)
    5. `gerrydb_etl/bootstrap/pl_geo_and_pop_substates.sh` (loads all substate level geo and pop)
