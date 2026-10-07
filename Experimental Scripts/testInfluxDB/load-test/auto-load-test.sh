#!/bin/bash
rootpath=`echo $INFLUXDB_TEST_HOME`
# 全局参数
#influxd_path=$rootpath/executable/influxd-old  # inluxdb数据库启动器路径
influxd_path=$rootpath/executable/influxd-optimized  # inluxdb数据库启动器路径


influxdb_data_path=$rootpath/option/influxdb-data/.influxdb # influxdb数据库数据所在目录
#influxdb_data_path=/home/luwenqi/.influxdb/data # influxdb数据库数据所在目录
influxd_conf_path=$rootpath/option/influxd.conf # influxdb配置文件路径

test_data_path=$rootpath/data # 测试数据文件夹路径

test_log_path=$rootpath/log # 测试的日志文件夹路径

insert_result_path=$rootpath/result/tsbs-insert # tsbs的load的测试结果输出文件夹
cache_result_path=$rootpath/result/cache # 收集的cache
index_result_path=$rootpath/result/index # 收集的index
time_result_path=$rootpath/result/time # 收集的cache
tsm_result_path=$rootpath/result/tsm #tsm文件的收集文件夹
query_result_path=$rootpath/result/tsbs-query # 收集的cache

tsbs_load_util=$rootpath/executable/tsbs_load_influx # tsbs装载工具
influx_cli=$rootpath/executable/influx # influxdb客户端工具

# 脚本参数
args_select_type=$1 # 选择测试的类型 cpu-only devops iot
args_scale=$2 # 数据集规模

# 测试类型
ts_type=('cpu-only' 'devops' 'iot')

# 清空result文件夹和log文件夹
clear_env(){
    # log和result按类别创建文件
    rm -rf $insert_result_path/*
	rm -rf $cache_result_path/*
	rm -rf $index_result_path/*
	rm -rf $time_result_path/*
	rm -rf $tsm_result_path/*
	rm -rf $query_result_path/*
        for i in {0..2}
        do
		# 跳过不参加测试的文件夹
		if [ -n "$args_select_type"  ]; then
                    cur_type=${ts_type[$i]}
                    if [ "$args_select_type" != "$cur_type" ]; then
                        continue
                    fi
	        fi

                mkdir -p $insert_result_path/${ts_type[$i]}
		mkdir -p $cache_result_path/${ts_type[$i]}
		mkdir -p $index_result_path/${ts_type[$i]}
		mkdir -p $time_result_path/${ts_type[$i]}
		mkdir -p $tsm_result_path/${ts_type[$i]}
		mkdir -p $query_result_path/${ts_type[$i]}

        done

	ab_log_path=$test_log_path"/"$args_select_type
	rm -rf $ab_log_path/*
        mkdir -p $ab_log_path	
}


# 打印日志到日志文件函数 参数一：测例类型 参数二：数据文件 参数三：日志
log_and_echo(){
        use_case_type=$1
        use_case_name=$2
        msg=$3
        log_file=$test_log_path"/"$use_case_type"/"$use_case_name".log"
        echo $msg
        echo $msg >> $log_file
}


# 关闭数据库并清理环境
kill_influxd(){
    influxd_port=`ps aux|grep influxd-optimized | head -n 1 | awk '{print $2}'`
	#influxd_port=`ps aux|grep influxdb3 | head -n 1 | awk '{print $2}'`
	echo $influxd_port
    kill -9 $influxd_port
    rm -rf $influxdb_data_path
    sleep 5
}

# 启动数据库
start_influxd(){
    $influxd_path -config $influxd_conf_path &
	# $influxd_path --storage-cache-snapshot-memory-size 67108864
    sleep 5

# 	/home/luwenqi/.influxdb/influxdb3 serve \
#   --node-id="node0" \
#   --http-bind="0.0.0.0:8086" \
#   --object-store="file" \
#   --data-dir="/home/luwenqi/.influxdb/data"  &
#   sleep 5
}

# 收集cache、index、time、tsm的结果 参数一：测例类型 参数二：数据文件
collect_metric(){
	cache_file_path=$rootpath/option/res/cache/cache.log # cache统计内容
	cache_time_file_path=$rootpath/option/res/cache/cache_time.log # cache 刷盘统计内容
	index_file_path=$rootpath/option/res/index/index.log # index统计输出路径
	index_time_file_path=$rootpath/option/res/index/index_time.log # index维护时间内容
	
	use_case_type=$1
    use_case_name=$2

	cache_mv_path=$cache_result_path"/"$use_case_type"/"$use_case_name"-cache.log"
	cache_time_mv_path=$cache_result_path"/"$use_case_type"/"$use_case_name"-cache_time.log"
	index_mv_path=$index_result_path"/"$use_case_type"/"$use_case_name"-index.log"
	time_mv_path=$time_result_path"/"$use_case_type"/"$use_case_name"-time.log"

	tsm_mv_path=$tsm_result_path"/"$use_case_type"/"$use_case_name"-tsm.log"

	if [ -e "$cache_file_path" ]; then
  		mv $cache_file_path $cache_mv_path
	fi

	if [ -e "$cache_time_file_path" ]; then
                mv $cache_time_file_path $cache_time_mv_path
        fi


	if [ -e "$index_file_path" ]; then
                mv $index_file_path $index_mv_path
        fi


	if [ -e "$index_time_file_path" ]; then
                mv $index_time_file_path $time_mv_path
        fi

	data_dir="$rootpath/option/influxdb-data/.influxdb/data/lwq/autogen"
	#data_dir="/home/luwenqi/.influxdb/data"

	# 清空日志文件
	echo "" > $tsm_mv_path

	# 遍历目录
	#for folder in "$data_dir"/*; do
  #		if [ -d "$folder" ]; then
  #  		# 对每个子目录执行 ls -lh，并将结果追加到日志文件
  #  		ls -lh "$folder" >> $tsm_mv_path
# 		 fi
#		done
	# echo "#################################" >>$tsm_mv_path
	du -sh "$rootpath/option/influxdb-data/.influxdb" >> $tsm_mv_path
	#du -sh "/home/luwenqi/.influxdb/data" >> $tsm_mv_path
	#du -sh "/home/nxl/.influxdbv2" >> $tsm_mv_path
	#ls -lh  $rootpath/option/influxdb-data/.influxdb/data/nxl/autogen/1 >> $tsm_mv_path
}

# 测试函数：
auto_test(){
	for i in {0..2}
        do
            if [ -n "$args_select_type"  ]; then
                    cur_type=${ts_type[$i]}
                    if [ "$args_select_type" != "$cur_type" ]; then
                        echo "$cur_type is not enter test"
                        continue
                    fi
            fi
            #for file in `ls $test_data_path"/"${ts_type[$i]}`
            #do
	    	file=influx-${ts_type[$i]}-$args_scale
		ab_data_path=$test_data_path"/"$cur_type"/"$file  # 数据文件绝对路径
                ab_res_path=$insert_result_path"/"$cur_type"/"$file"-result.txt" # 测试日志文件
		ab_log_path=$test_log_path"/"$cur_type"/"$file".log"

		# 清理测试环境
		log_and_echo $cur_type $file "关闭influxd进程"
  	    kill_influxd

        # 启动测试数据库
		log_and_echo $cur_type $file "启动influxdb"
        start_influxd

		# 启动命令行工具，创建数据库nxl
		log_and_echo $cur_type $file "创建数据库"
		$influx_cli -execute "create database lwq"
		#influxdb3 create database nxl --host http://127.0.0.1:8086
        
       	# 开始测试
		log_and_echo $cur_type $file "开始测试"
		COMMAND="$tsbs_load_util --file $ab_data_path --batch-size 10000 --db-name "lwq" --results-file $ab_res_path --workers 1"
		echo $COMMAND
		$tsbs_load_util --file $ab_data_path --batch-size 10000 --db-name "lwq" --results-file $ab_res_path --workers 1 >> $ab_log_path # 此处要指定数据库nxl
		
		# 收集结果
		log_and_echo $cur_type $file "收集结果"
		collect_metric $cur_type $file

		# 测试结束(这里插入数据完成时需要进行查询测试，所以不能关闭数据库)
		log_and_echo $cur_type $file "测试完成，等待剩余数据刷盘"
		#kill_influxd

		break
	    #done
	done
}

clear_env
auto_test
