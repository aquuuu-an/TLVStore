#!/bin/bash

rootpath=`echo $INFLUXDB_TEST_HOME`

start_cpu_script=$rootpath/start.sh
start_devops_script=$rootpath/start.sh
start_iot_script=$rootpath/start.sh
all_res_dir=$rootpath/all_res
option_path=$rootpath/option/option.tpl

pak_path=$rootpath/pak_final_result.sh

#优化的cpu
sed -i '1s/SelectGroup=false/SelectGroup=true/' $option_path
$start_cpu_script cpu-only
rm -f $all_res_dir"/cpu-only-group-final-result.tar.gz"
mv $rootpath/final-result.tar.gz $all_res_dir"/cpu-only-group-final-result.tar.gz"



