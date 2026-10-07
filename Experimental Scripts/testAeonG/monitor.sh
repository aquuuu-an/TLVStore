#!/bin/bash
# monitor.sh [start|stop] [scale]

LOG_DIR="./logs/metrics"
mkdir -p "$LOG_DIR"

if [ "$1" == "start" ]; then
    SCALE=$2
    FILE="$LOG_DIR/metrics_$SCALE.csv"
    echo "Timestamp,CPU_User(%),Mem_Used(%),Disk_Read(KB/s),Disk_Write(KB/s)" > "$FILE"

    (
    while true; do
        TIME=$(date +%H:%M:%S)

        # CPU user %
        CPU=$(top -bn1 | awk -F',' '/Cpu/ {print $1}' | awk '{print $2}')

        # Memory usage %
        MEM=$(free -m | awk '/^Mem:/ { printf "%.2f", $3/$2*100 }')
        MEM_PERCENT=$(free | awk '/^Mem:/ {
            used = $3;
            total = $2;
            if (total > 0) {
                printf "%.2f", (used/total)*100
            } else {
                printf "0.00"
            }
        }')
        
        # 如果上面失败，尝试使用/proc/meminfo
        if [ -z "$MEM_PERCENT" ] || [ "$MEM_PERCENT" = "0.00" ]; then
            MEM_PERCENT=$(awk '
                /MemTotal:/ {total=$2}
                /MemAvailable:/ {available=$2}
                END {
                    if (total > 0) {
                        used = total - available;
                        printf "%.2f", (used/total)*100
                    } else {
                        printf "0.00"
                    }
                }
            ' /proc/meminfo)
        fi
        
        MEM=$MEM_PERCENT

        # Disk IO - 自动检测磁盘设备
        # 找到系统根分区所在的磁盘
        ROOT_DISK=$(df / | tail -1 | awk '{print $1}' | sed 's/\/dev\///')
        
        # 如果没有找到，尝试常见设备名
        if [ -z "$ROOT_DISK" ] || [ ! -b "/dev/$ROOT_DISK" ]; then
            # 尝试常见磁盘设备
            for disk in sda vda xvda nvme0n1; do
                if [ -b "/dev/$disk" ]; then
                    ROOT_DISK="$disk"
                    break
                fi
            done
        fi
        
        if [ -n "$ROOT_DISK" ]; then
            DISK=$(iostat -dk "$ROOT_DISK" 1 2 2>/dev/null | grep "^$ROOT_DISK" | tail -1 | awk '{printf "%.2f,%.2f", $3, $4}')
        else
            DISK="0.00,0.00"
        fi

        echo "$TIME,$CPU,$MEM,$DISK" >> "$FILE"
        sleep 2
    done
    ) &

    echo $! > /tmp/monitor_perf.pid
    echo "Performance monitoring started for scale $SCALE."

elif [ "$1" == "stop" ]; then
    if [ -f /tmp/monitor_perf.pid ]; then
        PID=$(cat /tmp/monitor_perf.pid)
        kill "$PID" && rm /tmp/monitor_perf.pid
        echo "Performance monitoring stopped."
    fi
fi