#!/bin/bash
rootpath=`echo $INFLUXDB_TEST_HOME`
option_path=$rootpath/option
# 删除两个配置文件
rm -f $option_path/influxd.conf
rm -f $option_path/option

#生成文件
cp $option_path/influxd.conf.tpl $option_path/influxd.conf
cp $option_path/option.tpl $option_path/option
sed -i "s#rootpath#$rootpath#g" $option_path/influxd.conf
sed -i "s#rootpath#$rootpath#g" $option_path/option

