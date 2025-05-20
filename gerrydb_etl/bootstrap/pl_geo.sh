#!/bin/bash

### Bootstraps a new GerryDB instance with core Census geographic data. ###
# Usage: ./pl_geo.sh <state FIPS code> <log dir>
# must be used after pl_geo_states.sh

fip=$1
log_dir=$2

error_log="pl_geo_python_err.log"
level_log="failed_levels.log"
error_raised=0

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
        mkdir -p "${log_dir}/${fip}/${year}/${level}"

        echo "Loading geographies: FIPS $fip, $year Census, $level level..."
        python -m gerrydb_etl.bootstrap.pl_geo \
            --namespace "census.$year" \
            --fips $fip \
            --year $year \
            --level $level \
            2>> "${log_dir}/${fip}/${year}/${level}/${error_log}"
            
            if [ $? -eq 1 ]
            then
                echo $year $level >> "${log_dir}/${fip}/${level_log}"
                error_raised=1
            fi 
    done
done

exit $error_raised