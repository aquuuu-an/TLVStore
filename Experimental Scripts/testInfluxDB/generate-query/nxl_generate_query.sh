#!/bin/bash
rootpath=`echo $INFLUXDB_TEST_HOME`
###############################################################################
###                          此脚本用来生成TSBS查询                         ###
###############################################################################
#select=$1 #选择哪个时间，和生成的数据要对应
# 设备规模
#ts_scale=(10000 20000 30000 40000 50000 60000 70000 80000 90000 100000)


# 时间范围i
#ts_start_time=('2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' '2023-01-01T00:00:00Z' )
#ts_end_time=('2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z' '2023-01-04T00:00:00Z')
#ts_end_time=('2023-01-02T00:00:00Z' '2023-01-01T12:00:00Z' '2023-01-01T8:00:00Z' '2023-01-01T06:00:00Z' '2023-01-01T04:48:00Z' '2023-01-01T04:00:00Z' '2023-01-01T03:25:00Z' '2023-01-01T03:00:00Z' '2023-01-01T02:40:00Z' '2023-01-01T02:00:00Z')

args_select_type=$1
args_start_time=$2
args_end_time=$3
args_scale=$4

# 设置参数
group_count=5 # 执行8组查询
query_count_every_group=20

SEED=123456
SCALE=$args_scale
START_TIME=$args_start_time
END_TIME=$args_end_time
FORMAT="influx"
OUTPUT_DIR="$rootpath/generate-query/query"
util=$rootpath/executable/tsbs_generate_queries

seeds=(97627 8765 86707 27466 44260 27488 45661 95712 89678 83546 36816 3334 35016 90281 13033 32623 14511 51926 5620 31233
 85909 75887 16467 59614 9331 66981 82191 55846 71232 41027 91706 8882 38162 51594 13691 81743 90008 7994 17552 4172
 19130 84017 47762 45498 50000 68665 56234 48576 49960 42626 95210 43512 35003 93900 26822 39858 92343 53630 38760 37218
 17615 25840 18963 56656 36109 40705 72496 55293 91866 2086 20140 47941 2517 45600 82851 62445 75407 29809 44093 3361
 49008 26594 49382 71490 84706 7284 17404 52035 48330 5737 5481 19943 30207 25246 93632 93105 58499 61232 45461 1128
 1894 46519 70685 14508 50274 88859 34005 39633 29866 71906 8766 90023 62931 58124 81429 97403 26164 63123 52057 13126
 23538 17770 32198 89368 23748 38456 76590 80481 31838 30425 77151 38844 90880 80042 22768 67888 46782 25663 33893 33276
 83435 71858 19177 38646 5662 46378 18595 48272 90167 20118 45570 14351 88323 87060 28682 43937 84778 39195 52488 28091
 85834 66769 87202 73277 62909 78506 10103 99539 70845 78350 7134 70264 52263 65197 69059 19697 63557 69121 36788 29275
 40974 94656 73834 29538 88048 32653 37010 8823 27870 28834 55683 78804 34780 19732 91354 25216 99839 96491 80280 29642)

# 清空query文件夹
clear_env(){
        rm -rf $OUTPUT_DIR/*
}

clear_env

# 遍历不同的 use-case 和 query-type
for TYPE in "cpu-only" "devops" "iot"; do

 if [ "$args_select_type" != "$TYPE" ]; then
        echo "$TYPE is not enter test"
                 continue
 fi
cpu_devops_list=("single-groupby-1-1-1" "single-groupby-1-8-1" "single-groupby-5-1-1" "single-groupby-5-8-1")
# cpu_devops_list=("single-groupby-1-1-1" "single-groupby-1-1-12" "single-groupby-1-8-1" "single-groupby-5-1-1" "single-groupby-5-1-12" "single-groupby-5-8-1" "double-groupby-5" "single-groupby-all" "high-cpu-all" "high-cpu-1" "lastpoint")
#cpu_devops_list=("single-groupby-1-1-1" "single-groupby-1-1-12" "single-groupby-1-8-1" "single-groupby-5-1-1" "single-groupby-5-1-12" "single-groupby-5-8-1" "cpu-max-all-1" "cpu-max-all-8"  "double-groupby-1" "double-groupby-5" "double-groupby-all" "high-cpu-all" "high-cpu-1" "lastpoint" "groupby-orderby-limit")
#cpu_devops_list=("single-groupby-1-1-1" "single-groupby-1-1-12" "single-groupby-1-8-1" "single-groupby-5-1-1" "single-groupby-5-1-12" "single-groupby-5-8-1" "cpu-max-all-1" "cpu-max-all-8"  "double-groupby-1" "double-groupby-5")
# cpu_devops_list=("high-cpu-all" "groupby-order-limit")
#cpu_devops_list=("single-groupby-1-1-1" "single-groupby-1-1-12" "single-groupby-1-8-1" "single-groupby-5-1-1" "single-groupby-5-1-12" "single-groupby-5-8-1" "cpu-max-all-1" "cpu-max-all-8")


iot_list=("last-loc" "high-load" "avg-load" "breakdown-frequency")
#iot_list=("last-loc" "high-load" "avg-load" "avg-vs-projected-fuel-consumption" "avg-daily-driving-duration" "daily-activity" "breakdown-frequency")
#iot_list=("last-loc" "low-fuel" "high-load" "stationary-trucks" "long-driving-sessions" "long-daily-sessions" "avg-vs-projected-fuel-consumption" "avg-daily-driving-duration" "avg-daily-driving-session" "avg-load" "daily-activity" "breakdown-frequency")


 select_list=("${cpu_devops_list[@]}")
 if [ "$args_select_type" = "iot" ]; then
          select_list=("${iot_list[@]}")
 fi

 #创建组文件夹
for ((group_index=1; group_index<=$group_count; group_index++)); do

	# 组文件夹
	group_dir=$OUTPUT_DIR/$group_index
    mkdir -p $group_dir

 	for query in "${select_list[@]}"; do
	 if [ "$query" = "lastpoint" ]; then
		  real_query=1
   	 else
        	  real_query=$query_count_every_group
   	 fi

   	 if [ "$args_select_type" = "iot" ]; then
        	  real_query=1
   	 fi

	#生成随机种子
	# 获取数组长度
	array_length=${#seeds[@]}
	random_index=$((RANDOM % array_length))
	random_seed=${seeds[$random_index]}
	echo "seeds= " $random_seed

   	# 生成命令
   	COMMAND="$util --use-case=\"$TYPE\" --seed=$random_seed --scale=$SCALE \
            --timestamp-start=\"$START_TIME\" --timestamp-end=\"$END_TIME\" \
             --queries=$real_query --query-type=\"$query\" --format=\"$FORMAT\" \
              > $group_dir/$query"

   	# 执行命令
    echo "Running command: $COMMAND"
   	eval $COMMAND

	# COMMAN="sed -i 's/max/mean/g' $group_dir/$query"
    #eval $COMMAN

 	done
done
done
