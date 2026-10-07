#!/bin/bash
# insert_engine_memgraph.sh [scale]

SCALE=$1
LOG_FILE="./logs/insert_summary_${SCALE}.log"

# Memgraph 默认 Bolt
BOLT_URL="bolt://localhost:7687"

# Memgraph 默认是无认证（除非你自己开了）
USER=""
PASS=""

echo "[$(date)] Starting TSBS-style Load (Memgraph): ${SCALE}" | tee -a "$LOG_FILE"

echo "【索引】确保唯一性约束存在（Memgraph）..."
# 增加重试机制，防止 memgraph 尚未完全启动
python3 ./insert_engine_memgraph.py 

# 执行模拟在线写入（注意 Python 脚本里 driver 也要换）
# python3 ./AeonG_tsbs_load.py --scale $SCALE >> "$LOG_FILE" 2>&1
python3 ./AeonG_tsbs_load.py --scale $SCALE
