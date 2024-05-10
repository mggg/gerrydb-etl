#!/bin/bash

### Bootstraps a new GerryDB instance with core Census geographic/pop data for substate level. ###
# Usage: ./pl_geo_and_pop_substates.sh <FIPS>
log_dir="logs"
failed_fips_log="pl_substates_failed_fips.log"
time_log="pl_substates_time.log"

# fips=("01" 
#"02" 
#"04" 
#"05" "06" "08" "09" "10" "11" 
# "12" "13" "15" "16" "17" "18" "19" "20" "21" "22" 
# "23" "24" "25" "26" "27" "28" "29" "30" "31" "32" 
# "33" "34" "35" "36" "37" "38" "39" "40" "41" "42" 
# "44" "45" "46" "47" "48" "49" "50" "51" "53" "54" 
# "55" "56")
fip=$1

# restore to save harddisk
/opt/homebrew/cellar/postgresql@16/16.2_1/bin/pg_restore -U postgres -h localhost -p 54320 -d gerrydb -c -Ft gerrydb_all_state_aiannh_geo_all_aiannh_pop.tar


echo "Loading geographies: FIPS $fip" 1>>  "${log_dir}/${time_log}" 
mkdir -p "${log_dir}/pl_geo/${fip}"

{ time ./pl_geo.sh $fip "${log_dir}/pl_geo" ;} 2>> "${log_dir}/${time_log}" 


if [ $? -eq 1 ]
then
    echo  $fip >> "${log_dir}/${failed_fips_log}"

fi 


echo "Loading populations: FIPS $fip" 1>>  "${log_dir}/${time_log}"
mkdir -p "${log_dir}/pl_pop/${fip}"

{ time ./pl_pop.sh $fip "${log_dir}/pl_pop" ; } 2>> "${log_dir}/${time_log}" 


if [ $? -eq 1 ]
then
    echo  $fip >> "${log_dir}/${failed_fips_log}"

fi


