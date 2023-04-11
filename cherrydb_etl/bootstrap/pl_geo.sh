#!/bin/bash

### Bootstraps a new CherryDB instance with core Census geographic data. ###
# Usage: ./pl_geo.sh <state FIPS code>

years=( "2010" "2020" )
levels=(
    "state"
    "county"
    "tract"
    "bg"
#   "block"
    "vtd"
    "place"
    "cousub"
#   "aiannh" 
)

for year in "${years[@]}"
do
    for level in "${levels[@]}"
    do
        echo "Loading geographies: FIPS $1, $year Census, $level level..."
        python -m cherrydb_etl.bootstrap.pl_geo \
            --namespace "census.$year" \
            --fips $1 \
            --year $year \
            --level $level 
    done
done
