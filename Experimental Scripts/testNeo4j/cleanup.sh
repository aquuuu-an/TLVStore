#!/bin/bash
# cleanup.sh

NEO4J_PASS="12345678"

echo "【停止】关闭 Neo4j..."
neo4j stop || true
sleep 2

PID=$(pgrep -f "org.neo4j.server.CommunityEntryPoint")
if [ -n "$PID" ]; then
    echo "【强制】杀掉残留 Neo4j 进程: $PID"
    sudo kill -9 $PID
fi

echo "【清理】删除数据库文件..."
sudo rm -rf /var/lib/neo4j/data/databases/neo4j
sudo rm -rf /var/lib/neo4j/data/transactions/neo4j

echo "【启动】重新启动 Neo4j..."
sudo neo4j start

echo "【就绪检测】等待 Bolt 端口 (17687)..."
until nc -z localhost 17687; do
    sleep 2
    echo -n "."
done
echo ""

echo "【就绪检测】等待 system DB 初始化..."
sleep 8

# echo "【认证】使用 Cypher 强制改密（非交互）..."

# cypher-shell -a "bolt://localhost:17687" \
#   -u neo4j -p neo4j \
#   "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO '$NEO4J_PASS';" \
#   >/dev/null 2>&1 || true

# echo "【验证】检查新密码是否可用..."
# until cypher-shell -a "bolt://localhost:17687" -u neo4j -p "$NEO4J_PASS" "RETURN 1" >/dev/null 2>&1
# do
#     echo "  等待认证生效..."
#     sleep 2
# done

# echo "Neo4j 已完全就绪，密码已更新为 $NEO4J_PASS"
