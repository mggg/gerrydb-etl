#!/bin/bash

### Bootstraps a new GerryDB instance with core Census geographic data. ###
# Usage: ./pl_geo.sh <state FIPS code>
# must be used after pl_geo_states.sh

years=( "2010" "2020" )
levels=(
    "county"
    "tract"
    "bg"
    "vtd"
    "place"
    "cousub"
    "block" # by far the longest import (2010, alabama, 252k blocks, 30 min; 2020 AL 20 min) 
)

for year in "${years[@]}"
do
    for level in "${levels[@]}"
    do
        echo "Loading geographies: FIPS $1, $year Census, $level level..."
        python -m gerrydb_etl.bootstrap.pl_geo \
            --namespace "census.$year" \
            --fips $1 \
            --year $year \
            --level $level 
    done
done
