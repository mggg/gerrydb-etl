#!/bin/bash

### Bootstraps a new GerryDB instance with core Census geographic data for state level. ###
# Usage: ./pl_geo_states.sh 
error_log="pl_geo_states_python_err.log"
failed_fips_log="pl_geo_states_failed_fips.log"

fips=("01" "02" "04" "05" "06" "08" "09" "10" "11" 
"12" "13" "15" "16" "17" "18" "19" "20" "21" "22" 
"23" "24" "25" "26" "27" "28" "29" "30" "31" "32" 
"33" "34" "35" "36" "37" "38" "39" "40" "41" "42" 
"44" "45" "46" "47" "48" "49" "50" "51" "53" "54" 
"55" "56")
years=( "2010" "2020" )


for fip in "${fips[@]}"
do
    for year in "${years[@]}"
    do
            echo "Loading geographies: FIPS $fip, $year Census, $level level..."
            python -m gerrydb_etl.bootstrap.pl_geo \
                --namespace "census.$year" \
                --fips $fip \
                --year $year \
                --level "state" \
                2>> $error_log
            if [ $? -eq 1 ]
            then
                echo $year $fip $table >> $failed_fips_log

            fi 
        
    done
done
