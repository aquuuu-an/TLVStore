#!/usr/bin/env bash
set -e

DB_NAME=neo4j
NEO4J_HOME=/var/lib/neo4j
NODES="/var/lib/neo4j/import/influx-cpu-only-10-nodes.csv"
EDGES="/var/lib/neo4j/import/influx-cpu-only-10-edges.csv"


echo "========== Neo4j Bulk Import Benchmark =========="

# ---------- 清空旧数据库 ----------
echo "[1/4] Removing old database..."
# rm -rf $NEO4J_HOME/data/databases/$DB_NAME
# rm -rf $NEO4J_HOME/data/transactions/$DB_NAME

# ---------- 统计数据规模 ----------
NODE_COUNT=$(($(wc -l < $NODES) - 1))
EDGE_COUNT=$(($(wc -l < $EDGES) - 1))

echo "Nodes: $NODE_COUNT"
echo "Edges: $EDGE_COUNT"

# ---------- 开始计时 ----------
echo "[2/4] Starting import..."
START=$(date +%s.%N)

/usr/share/neo4j/bin/neo4j-admin database import full neo4j \
  --overwrite-destination=true \
  --nodes=Vertex=/var/lib/neo4j/import/influx-cpu-only-10-nodes.csv \
  --relationships=CALL=/var/lib/neo4j/import/influx-cpu-only-10-edges.csv \
  --report-file=/var/lib/neo4j/import/import.report \
  --delimiter=',' \
  --quote='"'


END=$(date +%s.%N)

# ---------- 计算 ----------
ELAPSED=$(echo "$END - $START" | bc)

NODE_TP=$(echo "scale=2; $NODE_COUNT / $ELAPSED" | bc)
EDGE_TP=$(echo "scale=2; $EDGE_COUNT / $ELAPSED" | bc)

echo "========== RESULT =========="
echo "Time: $ELAPSED s"
echo "Node throughput: $NODE_TP nodes/s"
echo "Edge throughput: $EDGE_TP edges/s"
echo "=============================================="
