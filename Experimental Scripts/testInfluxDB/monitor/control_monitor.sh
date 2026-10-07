#!/bin/bash
rootpath=`echo $INFLUXDB_TEST_HOME`
# 设置监控脚本的路径
monitor_script_path=$rootpath/monitor/monitor.sh


cmd=$1
use_case=$2
scale=$3
# 启动monitor.sh脚本
start_monitor() {
    if [ -f "$monitor_script_path" ]; then
        $monitor_script_path $use_case $scale &
        echo "monitor.sh 启动成功"
    else
        echo "错误：找不到 monitor.sh 脚本文件"
    fi
}

# 停止monitor.sh脚本
stop_monitor() {
    monitor_pid=$(pgrep -f "monitor.sh" | head -n 1)
    echo $monitor_pid
    if [ -n "$monitor_pid" ]; then
        kill  "$monitor_pid"
        echo "monitor.sh 已停止"
    else
        echo "monitor.sh 未在运行"
    fi
}

# 显示使用说明
usage() {
    echo "Usage: $0 start|stop"
}

# 根据命令行参数启动或停止monitor.sh
case "$cmd" in
    "start")
        start_monitor
        ;;
    "stop")
        stop_monitor
        ;;
    *)
        usage
        exit 1
        ;;
esac

exit 0
