# #!/bin/bash
# # insert_engine.sh [scale]

# SCALE=$1
# LOG_FILE="./logs/insert_summary_${SCALE}.log"
# BOLT_URL="bolt://localhost:17687"
# USER="neo4j"
# PASS="12345678"

# echo "[$(date)] Starting TSBS-style Load: ${SCALE}" | tee -a "$LOG_FILE"

# echo "【索引】确保唯一性约束存在..."
# # 增加重试机制，防止启动瞬间连接失败
# # for i in {1..10}; do
# #     cypher-shell -a "$BOLT_URL" -u "$USER" -p "$PASS" \
# #     "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE;" && break || sleep 5
# # done

# for i in {1..10}; do
#     cypher-shell -a "$BOLT_URL" -u "$USER" -p "$PASS" <<EOF && break || sleep 5
# CREATE CONSTRAINT node_id_unique IF NOT EXISTS
# FOR (n:Node)
# REQUIRE n.id IS UNIQUE;

# CREATE INDEX edge_ts_index IF NOT EXISTS
# FOR ()-[r:CONNECTED]-()
# ON (r.ts);
# EOF
# done
# # 执行模拟在线写入
# python3 ./neo4j_tsbs_load.py --scale $SCALE >> "$LOG_FILE" 2>&1


#!/bin/bash
# insert_engine.sh [scale]

SCALE=$1

if [ -z "$SCALE" ]; then
    echo "Usage: $0 [scale]"
    exit 1
fi

mkdir -p logs

LOG_FILE="./logs/insert_summary_${SCALE}.log"
BOLT_URL="bolt://localhost:17687"
USER="neo4j"
PASS="12345678"

echo "[$(date)] Starting TSBS-style Load: ${SCALE}" | tee -a "$LOG_FILE"

echo "[$(date)] Initializing indexes..." | tee -a "$LOG_FILE"

# 重试连接数据库
for i in {1..10}; do
    cypher-shell -a "$BOLT_URL" -u "$USER" -p "$PASS" <<EOF >> "$LOG_FILE" 2>&1 && break || sleep 5
CREATE CONSTRAINT node_id_unique IF NOT EXISTS
FOR (n:Node)
REQUIRE n.id IS UNIQUE;

CREATE INDEX edge_ts_index IF NOT EXISTS
FOR ()-[r:CONNECTED]-()
ON (r.ts);

CREATE INDEX edge_service_index IF NOT EXISTS
FOR ()-[r:CONNECTED]-()
ON (r.service);
EOF
done

echo "[$(date)] Index initialization finished" | tee -a "$LOG_FILE"

echo "[$(date)] Start data loading..." | tee -a "$LOG_FILE"

python3 ./neo4j_tsbs_load.py --scale "$SCALE" >> "$LOG_FILE" 2>&1

echo "[$(date)] Load finished." | tee -a "$LOG_FILE"