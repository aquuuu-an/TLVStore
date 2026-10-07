#!/bin/bash
rootpath=`echo $INFLUXDB_TEST_HOME`
args_use_case=$1
args_scale=$2

# 设置监控的进程名称
process_name="influxd-optimiz"
# process_name="influxdb3"

# 设置文件夹路径
folder_path="$rootpath/monitor/data"

while true; do
    # 获取当前时间戳
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")

    # 使用ps命令获取进程的CPU和内存利用率
    process_info=$(ps aux | grep "$process_name" | grep -v grep)

    # 提取CPU利用率和内存利用率
    cpu_usage=$(echo "$process_info" | awk '{print $3}')
    memory_usage=$(echo "$process_info" | awk '{print $4}')

    # 使用 iostat 获取磁盘读写速率信息
    disk_info=$(iostat -d -k 1 1 | grep -E 'sda')  # 替换为你的实际磁盘设备名

    # 提取磁盘读写速率
    disk_read=$(echo "$disk_info" | awk '{print $6}')
    disk_write=$(echo "$disk_info" | awk '{print $7}')

    # 创建新的txt文件
    current_file=$folder_path"/"$args_use_case"-"$args_scale"-monitor.txt"
    touch "$current_file"

    # 写入内容到文件
    #echo "$timestamp | $process_name CPU: $cpu_usage% | Mem: $memory_usage%" >> "$current_file"
    echo "$timestamp | $process_name CPU: $cpu_usage% | Mem: $memory_usage% | Disk Read: $disk_read KB/s | Disk Write: $disk_write KB/s" >> "$current_file"

    # 等待一秒
    sleep 1
done

