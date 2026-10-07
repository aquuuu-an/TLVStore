#!/bin/bash
rootpath=`cat ./rootpath`
# 获取正在运行的auto-load-test.sh进程的PID列表
# 获取auto-load-test.sh进程的PID列表
pids=$(pgrep -f "auto-load-test.sh")

if [ -n "$pids" ]; then
    # 遍历PID列表，逐个终止进程
    for pid in $pids; do
        kill -9 "$pid"
        echo "进程号 $pid 的 auto-load-test.sh 进程已终止"
    done
else
    echo "未找到匹配的 auto-load-test.sh 进程"
fi

pids=$(pgrep -f "tsbs_load_influx")

if [ -n "$pids" ]; then
    # 遍历PID列表，逐个终止进程
    for pid in $pids; do
        kill -9 "$pid"
        echo "进程号 $pid 的 tsbs_load_influx 进程已终止"
    done
else
    echo "未找到匹配的 tsbs_load_influx 进程"
fi

pids=$(pgrep -f "auto-query-test.sh")

if [ -n "$pids" ]; then
    # 遍历PID列表，逐个终止进程
    for pid in $pids; do
        kill -9 "$pid"
        echo "进程号 $pid 的 auto-query-test.sh 进程已终止"
    done
else
    echo "未找到匹配的 auto-query-test.sh 进程"
fi

pids=$(pgrep -f "start.sh")

if [ -n "$pids" ]; then
    # 遍历PID列表，逐个终止进程
    for pid in $pids; do
        kill -9 "$pid"
        echo "进程号 $pid 的 auto-query-test.sh 进程已终止"
    done
else
    echo "未找到匹配的 auto-query-test.sh 进程"
fi


pids=$(pgrep -f "auto_test.sh")

if [ -n "$pids" ]; then
    # 遍历PID列表，逐个终止进程
    for pid in $pids; do
        kill -9 "$pid"
        echo "进程号 $pid 的 auto-query-test.sh 进程已终止"
    done
else
    echo "未找到匹配的 auto-query-test.sh 进程"
fi


pids=$(pgrep -f "monitor.sh")

if [ -n "$pids" ]; then
    # 遍历PID列表，逐个终止进程
    for pid in $pids; do
        kill -9 "$pid"
        echo "进程号 $pid 的 auto-query-test.sh 进程已终止"
    done
else
    echo "未找到匹配的 auto-query-test.sh 进程"
fi


pids=$(pgrep -f "tsbs_run_queries_influx")

if [ -n "$pids" ]; then
    # 遍历PID列表，逐个终止进程
    for pid in $pids; do
        kill -9 "$pid"
        echo "进程号 $pid 的 tsbs_run_queries_influx 进程已终止"
    done
else
    echo "未找到匹配的 tsbs_run_queries_influx 进程"
fi



# 获取包含 "influxd" 的进程的 PID
pids=$(pgrep influxd)

# 检查是否有匹配的进程
if [ -n "$pids" ]; then
    # 使用循环杀死每个匹配的进程
    for pid in $pids; do
        echo "Killing process with PID: $pid"
        kill -9 $pid
    done
else
    echo "No processes containing 'influxd' found."
fi

pids=$(pgrep influxidb)

# 检查是否有匹配的进程
if [ -n "$pids" ]; then
    # 使用循环杀死每个匹配的进程
    for pid in $pids; do
        echo "Killing process with PID: $pid"
        kill -9 $pid
    done
else
    echo "No processes containing 'influxd' found."
fi
