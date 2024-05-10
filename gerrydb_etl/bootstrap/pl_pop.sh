#!/bin/bash

### Adds core Census population data to db. ###
# Usage: ./pl_pop.sh <state FIPS code>

fip=$1
log_dir=$2

error_log="pl_pop_python_err.log"
failed_log="failed_tables.log"
error_raised=0

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
    "block"
    )


for year in "${years[@]}"
do
    for level in "${levels[@]}"
    do
        for table in "${tables[@]}"
        do
            mkdir -p "${log_dir}/${fip}/${year}/${level}/$table"

            echo "Loading population data: FIPS $fip, $year Census, $level level, Table $table..."
            python -m gerrydb_etl.bootstrap.pl_pop_tables \
                --namespace "census.$year" \
                --fips $fip \
                --year $year \
                --level $level \
                --table $table\
                2>> "${log_dir}/${fip}/${year}/${level}/$table/$error_log"

            if [ $? -eq 1 ]
            then
                echo $year $level $table >> "${log_dir}/${fip}/${failed_log}"
                error_raised=1
            fi 
        done
    done
done

exit $error_raised