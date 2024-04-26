#!/bin/bash

### Bootstraps a new GerryDB instance with core Census population data for aiannh. ###
# Usage: ./pl_pop_state_aiannh.sh
# need all states and aiannh loaded using pl_geo_states and pl_geo_aiannh first


error_log="pl_pop_aiannh_python_err.log"
failed_fips_log="pl_pop_aiannh_failed_fips.log"
years=("2010" "2020") 
tables=( "P1" "P2" "P3" "P4" )



for year in "${years[@]}"
do
    if [ $year == "2010" ]
    then
        fips=("01" "02" "04"  
                "06" "08" "09" "10" 
                "12" "13" "15" "16" 
                "19" "20" 
                "22" 
                "23" 
                "25" "26" "27" "28" 
                "30" "31" "32" 
                "34" "35" "36" "37" "38" 
                "40" "41" 
                "44" "45" "46" 
                "48" "49" 
                "51" "53" 
                "55" "56")

    else # 2020
        fips=("01" "02" "04"  
                "06" "08" "09" "10"  
                "12" "13" "15" "16" 
                "18" 
                "19" "20" 
                "22" 
                "23"  
                "25" "26" "27" "28" 
                "30" "31" "32" 
                "34" "35" "36" "37" "38" 
                "40" "41" 
                "44" "45" "46" "47" 
                "48" "49" 
                "51" "53" 
                "55" "56"
                )
    fi
    for fip in "${fips[@]}"
    do  
        for table in "${tables[@]}"
        do
            echo "Loading population data: FIPS $fip, $year Census, $level level, Table $table..."
            python -m gerrydb_etl.bootstrap.pl_pop_tables \
                --namespace "census.$year" \
                --fips $fip \
                --year $year \
                --level "aiannh"  \
                --table $table\
                2>> $error_log
            
            if [ $? -eq 1 ]
            then
                echo $year $fip $table >> $failed_fips_log

            fi 
        done
    done
done


