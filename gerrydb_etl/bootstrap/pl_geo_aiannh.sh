#!/bin/bash

### Bootstraps a new GerryDB instance with core Census geographic data. ###
# Usage: ./pl_geo.sh <state FIPS code>

years=( "2010" "2020" )
error_log="pl_geo_aiannh_python_err.log"
failed_fips_log="pl_geo_aiannh_failed_fips.log"

for year in "${years[@]}"
do

    # different fips have AIANNH between 10 and 20
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
            echo "Loading geographies: FIPS $fip, $year Census, aiannh level..."
            python -m gerrydb_etl.bootstrap.pl_geo \
                --namespace "census.$year" \
                --fips $fip \
                --year $year \
                --level "aiannh"\
                2>> $error_log
            if [ $? -eq 1 ]
            then
                echo $year $fip $table >> $failed_fips_log

            fi 
        
    done
done