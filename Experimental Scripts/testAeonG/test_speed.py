# # test_speed.py
# import mgclient
# import time

# # 连接 Memgraph
# conn = mgclient.connect(host='127.0.0.1', port=7687)
# cursor = conn.cursor()

# print("创建测试节点...")

# # 方法1：使用 UNWIND 批量创建节点（推荐）
# nodes = [{"id": str(i)} for i in range(1, 10001)]
# cursor.execute("""
# UNWIND $nodes AS node
# MERGE (n:Node {id: node.id})
# """, {"nodes": nodes})
# conn.commit()

# print("测试边写入速度...")

# # 测试边写入速度
# batch_size = 10000
# edges = [{"source": str(i), "destination": str(i+1), "timestamp": i} 
#          for i in range(1, batch_size+1)]

# start = time.time()

# # 执行批量边写入
# cursor.execute("""
# UNWIND $edges AS e
# MATCH (a:Node {id: e.source})
# MATCH (b:Node {id: e.destination})
# CREATE (a)-[:CONNECTED {ts: e.timestamp}]->(b)
# """, {"edges": edges})
# conn.commit()

# elapsed = time.time() - start

# print(f"插入 {batch_size} 条边耗时: {elapsed:.2f} 秒")
# print(f"速度: {batch_size/elapsed:.0f} 条/秒")

# # 清理测试数据（可选）
# # cursor.execute("MATCH (n:Node) DETACH DELETE n")
# # conn.commit()
# # print("测试数据已清理")

# conn.close()

# test_minimal.py
# test_batch_small.py

# test_batch_large.py
import mgclient
import time
import sys

def test_batch_insert(batch_size):
    print(f"\n===== 测试批处理大小: {batch_size} =====")
    
    conn = mgclient.connect(host='127.0.0.1', port=7687)
    cursor = conn.cursor()
    
    # 准备数据
    nodes = [{"id": f"perf_test_{i}"} for i in range(batch_size)]
    
    # 测试1: 不使用MERGE（纯CREATE）
    print("测试1: 使用CREATE...")
    start = time.time()
    try:
        cursor.execute("""
        UNWIND $nodes AS node
        CREATE (n:Node {id: node.id})
        """, {"nodes": nodes})
        conn.commit()
        elapsed = time.time() - start
        speed = batch_size / elapsed
        print(f"  ✅ 耗时: {elapsed:.2f}秒, 速度: {speed:.0f} 节点/秒")
    except Exception as e:
        print(f"  ❌ 失败: {e}")
    
    # 清理数据
    cursor.execute("MATCH (n:Node) WHERE n.id STARTS WITH 'perf_test_' DELETE n")
    conn.commit()
    
    # 测试2: 使用MERGE
    print("测试2: 使用MERGE...")
    start = time.time()
    try:
        cursor.execute("""
        UNWIND $nodes AS node
        MERGE (n:Node {id: node.id})
        """, {"nodes": nodes})
        conn.commit()
        elapsed = time.time() - start
        speed = batch_size / elapsed
        print(f"  ✅ 耗时: {elapsed:.2f}秒, 速度: {speed:.0f} 节点/秒")
    except Exception as e:
        print(f"  ❌ 失败: {e}")
    
    conn.close()

# 测试不同大小
sizes = [1000, 5000, 10000, 20000, 50000]
for size in sizes:
    test_batch_insert(size)
    time.sleep(2)  # 给数据库一点喘息时间