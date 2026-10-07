#!/bin/bash
# 用例类型
rootpath=`echo $INFLUXDB_TEST_HOME`
#ts_type=('cpu-only' 'iot' 'devops')
args_select_type=$1
args_start_time=$2
args_end_time=$3
args_scale=$4
args_interval=$5

ts_type=('cpu-only' 'iot' 'devops')
# 设备规模
#ts_scale=(10000 20000 30000 40000 50000 60000 70000 80000 90000 100000)


# 时间范围i
#ts_start_time=('2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' )
#ts_end_time=('2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z' '2023-01-02T00:00:00Z')
#ts_end_time=('2023-01-02T00:00:00Z' '2023-01-01T12:00:00Z' '2023-01-01T8:00:00Z' '2023-01-01T06:00:00Z' '2023-01-01T04:48:00Z' '2023-01-01T04:00:00Z' '2023-01-01T03:25:00Z' '2023-01-01T03:00:00Z' '2023-01-01T02:40:00Z' '2023-01-01T02:00:00Z')

# 时间间隔
#ts_interval='120s'

# 生成路径 BASE_PATH/[ts_type]/influx-[ts-type]-[ts-scale]
ts_base_path=$rootpath/data

# 生成数据工具路径
tsbs_generate_data_util=$rootpath/executable/tsbs_generate_data

for i in {0..2}
do 

	cur_type=${ts_type[$i]}
	cur_select_type=$args_select_type
	if [ "$cur_select_type" != "$cur_type" ]; then
	echo "$cur_type is not enter test"
               	 continue
	fi
	#for j in {0..9}
	#do
		if [ ! -d "$ts_base_path/${ts_type[$i]}" ]; then
			mkdir -p $ts_base_path/${ts_type[$i]}
		fi
		# 如果要生成的文件存在，则不进行生成，直接退出
		cur_data_file=$ts_base_path/${ts_type[$i]}/influx-${ts_type[$i]}-$args_scale
		if [ -e "$cur_data_file" ]; then
			echo "数据已生成"
  		 	 break
		fi
		echo "Executing tsbs_generate_data command for " $cur_type  "  with scale " $args_scale 
		start_time=$(date +%s.%N)
		$tsbs_generate_data_util --use-case=${ts_type[$i]} --seed=123456 --scale=$args_scale --timestamp-start=$args_start_time --timestamp-end=$args_end_time --log-interval=$args_interval --format=influx > $ts_base_path/${ts_type[$i]}/influx-${ts_type[$i]}-$args_scale
		end_time=$(date +%s.%N)
	        execution_time=$(echo "$end_time - $start_time" | bc)
        	echo "Execution time: $execution_time seconds"
	#done
done




