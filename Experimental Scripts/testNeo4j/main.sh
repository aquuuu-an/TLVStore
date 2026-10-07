#!/bin/bash

chmod +x cleanup.sh monitor.sh insert_engine.sh
# 定义数据规模 (10 到 120)
ts_scale=(10 20 30 40 50 60 70 80 90 100 110 120)

for scale in "${ts_scale[@]}"
do
    echo ">>> [STARTING SCALE: ${scale}] <<<"

    # 1. 重置数据库
    bash ./cleanup.sh

    # 2. 开启监控
    bash ./monitor.sh start $scale

    # 3. 执行写入测试
    echo "Executing Insertion..."
    bash ./insert_engine.sh $scale

    # 4. 关闭监控
    bash ./monitor.sh stop

    # 5. 执行查询测试
    echo "Executing Queries..."
    python3 ./query_benchmark.py --scale $scale

    # 6. 数据统计汇总 (可选)
    # python3 scripts/aggregate_results.py --scale $scale

    echo ">>> [FINISHED SCALE: ${scale}M] <<<"
    echo ""
done