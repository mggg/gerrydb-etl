#!/bin/bash

### Bootstraps a new CherryDB instance with core Census data. ###

python -m cherrydb_etl.bootstrap.localities_base
python -m cherrydb_etl.bootstrap.localities_base