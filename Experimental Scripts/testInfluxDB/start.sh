#!/bin/bash
rootpath=`echo $INFLUXDB_TEST_HOME`
select_type=$1

# 所有时间范围 设备规模
ts_scale=(10 20 30 40 50 60 70 80 90 100)

ts_start_time=('2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z' '2025-01-01T00:00:00Z')
ts_end_time=('2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z' '2025-01-01T13:00:00Z')

ts_interval='60s'
if [ "$select_type" == "devops" ]; then
	ts_interval='600s'            
fi


generate_data_script=$rootpath/generate-data/generate-data.sh
generate_query_script=$rootpath/generate-query/nxl_generate_query.sh
insert_script=$rootpath/load-test/auto-load-test.sh
query_script=$rootpath/query-test/auto-query-test.sh
final_res_dir=$rootpath/final-result
tmp_res=$rootpath/result

test_log_path=$rootpath/log # 测试的日志文件夹路径

control_monitor_script=$rootpath/monitor/control_monitor.sh
monitor_data_dir=$rootpath/monitor/data



# 替换配置文件中的配置
option_path=$rootpath/option/sub.sh
$option_path

collect_data(){
	scale=$1 # 规模
	folder_path=$final_res_dir"/"$select_type"/"$scale
	mkdir -p "$folder_path"

	# 移入监控数据
	new_monitor_data_dir=$folder_path/monitor

	# 移入日志信息
	new_log_data_dir=$test_log_path

	# 把测试结果复制过来
	cp -r $tmp_res $folder_path
	cp -r $monitor_data_dir $new_monitor_data_dir
	cp -r $new_log_data_dir $folder_path
}



clear_test_result(){
	rm -rf $final_res_dir/*
	folder_path=$final_res_dir"/"$select_type
	if [ -d "$folder_path" ]; then
	    rm -r "$folder_path"
	fi	
	mkdir -p "$folder_path"
	rm -rf $rootpath/final-result.tar.gz	
}

clear_monitor_data(){
	rm -rf $rootpath/monitor/data/*

}

clear_log_data(){
	rm -rf $rootpath/log/*
}

# 关闭数据库并清理环境
kill_influxd(){
    influxd_port=`ps aux|grep influxd-optimized | head -n 1 | awk '{print $2}'`
	#influxd_port=`ps aux|grep influxdb3 | head -n 1 | awk '{print $2}'`
    kill -9 $influxd_port
}

pak(){
	tar -zcvf final-result.tar.gz $final_res_dir
}

遍历
main(){
	for j in {0..9}
        do
		# 生成数据
		# echo "$generate_data_script $select_type ${ts_start_time[$j]} ${ts_end_time[$j]} ${ts_scale[$j]} $ts_interval"
		# echo "【生成数据】" 
		# COMMAND="$generate_data_script $select_type ${ts_start_time[$j]} ${ts_end_time[$j]} ${ts_scale[$j]} $ts_interval"
		# echo $COMMAND
		# eval $COMMAND
		
		# # 生成查询
		# echo "【生成查询】"
		# $generate_query_script $select_type ${ts_start_time[$j]} ${ts_end_time[$j]} ${ts_scale[$j]}
		
		# # 开启监控
		echo "【开启监控】"
		$control_monitor_script start  $select_type ${ts_scale[$j]}

		# # 执行插入测试并收集结果
		echo "【吞吐量测试】"
		$insert_script $select_type ${ts_scale[$j]} 

		# # 关闭监控
        echo "【关闭监控】"
        $control_monitor_script stop

		# 执行查询并收集结果
		echo "【执行查询】"
		# $query_script $select_type
		python3 gsQuery.py --scale ${ts_scale[$j]}
		#python3 tquery.py > $final_res_dir"/"$select_type"/"query_results_${ts_scale[$j]}.txt


		# # 收集总结果
		collect_data ${ts_scale[$j]}
		clear_monitor_data
		
		# # 再次关闭influxdb
		kill_influxd

	done
}


clear_monitor_data
clear_log_data
clear_test_result
main
pak
