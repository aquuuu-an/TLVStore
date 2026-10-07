#!/bin/bash
# restart_memgraph_process.sh

CONTAINER_NAME="fb8bcbba1c30"  # 你的 Memgraph 容器 ID
MEMGRAPH_PATH="/home/AeonG/build/memgraph"  # Memgraph 二进制路径
BOLT_PORT=7687

echo "【停止】停止容器中的 Memgraph 进程..."

# 查找并停止 Memgraph 进程
docker exec $CONTAINER_NAME pkill -f memgraph || true
sleep 3

# 确保进程已完全停止
if docker exec $CONTAINER_NAME pgrep -f memgraph > /dev/null 2>&1; then
    echo "【强制】强制终止 Memgraph 进程..."
    docker exec $CONTAINER_NAME pkill -9 -f memgraph || true
    sleep 2
fi

echo "✅ Memgraph 进程已停止"

echo "【清理】清理数据文件（可选）..."

# 询问是否清理数据

echo "清理数据文件..."
docker exec $CONTAINER_NAME rm -rf /home/AeonG/build/mg_data/* 2>/dev/null || true
echo "✅ 数据已清理"


echo "【启动】重新启动 Memgraph 进程..."

# 在容器中启动 Memgraph 进程（后台运行）
docker exec -d $CONTAINER_NAME bash -c "cd /home/AeonG/build && ./memgraph --log-file /home/memgraph.log"

echo "【就绪检测】等待 Memgraph 启动并监听端口 $BOLT_PORT..."

# 等待端口可用
TIMEOUT=30
COUNTER=0
while ! nc -z localhost $BOLT_PORT 2>/dev/null; do
    sleep 1
    COUNTER=$((COUNTER + 1))
    echo -n "."
    if [ $COUNTER -ge $TIMEOUT ]; then
        echo ""
        echo "❌ 等待超时，Memgraph 可能启动失败"
        
        # 检查进程是否运行
        if docker exec $CONTAINER_NAME pgrep -f memgraph > /dev/null 2>&1; then
            echo "进程存在但端口未监听，检查日志："
            docker exec $CONTAINER_NAME ps aux | grep memgraph
        else
            echo "Memgraph 进程未运行"
        fi
        exit 1
    fi
done
echo ""

echo "【验证】检查 Memgraph 进程状态..."

# 检查进程
PROCESS_COUNT=$(docker exec $CONTAINER_NAME pgrep -f memgraph | wc -l)
if [ $PROCESS_COUNT -gt 0 ]; then
    echo "✅ Memgraph 进程正在运行 (PID: $(docker exec $CONTAINER_NAME pgrep -f memgraph | head -1))"
else
    echo "❌ Memgraph 进程未运行"
    exit 1
fi

# 测试连接
echo "【验证】测试 Bolt 连接..."
sleep 2
if nc -z localhost $BOLT_PORT 2>/dev/null; then
    echo "✅ Bolt 端口 $BOLT_PORT 已就绪"
else
    echo "❌ Bolt 端口 $BOLT_PORT 未就绪"
    exit 1
fi

echo ""
echo "🎉 Memgraph 已成功重新启动！"
echo "容器: $CONTAINER_NAME"
echo "进程: $(docker exec $CONTAINER_NAME pgrep -f memgraph)"