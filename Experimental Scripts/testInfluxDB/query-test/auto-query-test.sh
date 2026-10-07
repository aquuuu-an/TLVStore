#!/bin/bash
rootpath=`echo $INFLUXDB_TEST_HOME`
tsbs_run_query_util=$rootpath/executable/tsbs_run_queries_influx
query_result_dir=$rootpath/result/tsbs-query
query_dir=$rootpath/generate-query/query

args_select_type=$1

# 检查 query_dir 是否存在
if [ ! -d "$query_dir" ]; then
    echo "Error: Directory '$query_dir' not found."
    exit 1
fi



for folder in "$query_dir"/*/; do
    # 提取文件夹名称并输出
    folder_name=$(basename "$folder")

	cur_res_dir=$query_result_dir"/"$args_select_type"/"$folder_name
	mkdir -p $cur_res_dir

    # 遍历 query_dir 中的每个文件
	for file in "$query_dir/$folder_name"/*; do
    if [ -f "$file" ]; then
       	# 提取文件名（不包含路径）
       	file_name=$(basename "$file")
	 	echo $file_name
        
        # 构建完整的命令
       	command="$tsbs_run_query_util --db-name=lwq --file $file --results-file $query_result_dir"/"$args_select_type"/"$folder_name"/"$file_name.txt --workers 1"

        # 执行命令
       	echo "Executing command: $command"
       	$command
   	fi
	done
done
