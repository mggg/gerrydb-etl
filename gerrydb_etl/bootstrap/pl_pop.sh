#!/bin/bash

### Adds core Census population data to db. ###
# Usage: ./pl_pop.sh <state FIPS code>

years=(  "2010" "2020" )
tables=( "P1" "P2" "P3" "P4" )
levels=(
    "state"
    "county"
    "tract"
    "bg"
    "vtd"
    "place"
    "cousub"
    "block")


for year in "${years[@]}"
do
    for level in "${levels[@]}"
    do
        for table in "${tables[@]}"
        do
            echo "Loading population data: FIPS $1, $year Census, $level level, Table $table..."
            python -m gerrydb_etl.bootstrap.pl_pop_tables \
                --namespace "census.$year" \
                --fips $1 \
                --year $year \
                --level $level \
                --table $table
        done
    done
done