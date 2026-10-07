import mgclient
import random
import time

# =========================
# 配置
# =========================
HOST = "127.0.0.1"
PORT = 7687

NODE_COUNT = 10000        # 预生成节点数
EDGE_COUNT = 1000000       # 要测试的边数
BATCH_SIZE = 20000        # 每批大小（建议 10000~50000）

# =========================
# 连接
# =========================
conn = mgclient.connect(host=HOST, port=PORT)
cursor = conn.cursor()

print("Connected to Memgraph")

# =========================
# 清空数据库
# =========================
print("Cleaning database...")
cursor.execute("MATCH (n) DETACH DELETE n")
conn.commit()

# =========================
# 1️⃣ 创建测试节点
# =========================
print(f"Creating {NODE_COUNT} nodes...")

cursor.execute("""
UNWIND range(0, $count-1) AS id
CREATE (:Node {id: id})
""", {"count": NODE_COUNT})

conn.commit()

# =========================
# 2️⃣ 读取 internal id
# =========================
print("Loading internal node IDs...")

cursor.execute("MATCH (n:Node) RETURN id(n)")
node_ids = [record[0] for record in cursor.fetchall()]

print(f"Loaded {len(node_ids)} node internal IDs")

# =========================
# 3️⃣ 构造高性能建边语句
# =========================
CREATE_EDGE_FAST = """
UNWIND $batch AS row
MATCH (a) WHERE id(a) = row.a
MATCH (b) WHERE id(b) = row.b
CREATE (a)-[:CONNECTED {
    ts: row.ts,
    value: row.value
}]->(b)
"""

# =========================
# 4️⃣ 开始压测
# =========================
print(f"Creating {EDGE_COUNT} random edges...")

start_time = time.time()
total = 0
batch = []

for _ in range(EDGE_COUNT):

    a = random.choice(node_ids)
    b = random.choice(node_ids)

    batch.append({
        "a": a,
        "b": b,
        "ts": random.randint(1, 10_000_000),
        "value": random.randint(1, 100)
    })

    if len(batch) >= BATCH_SIZE:
        cursor.execute(CREATE_EDGE_FAST, {"batch": batch})
        total += len(batch)
        batch.clear()
        print(f"Progress: {total}", end="\r")

# 最后一批
if batch:
    cursor.execute(CREATE_EDGE_FAST, {"batch": batch})
    total += len(batch)

conn.commit()

elapsed = time.time() - start_time

print("\n===== TEST RESULT =====")
print(f"Edges created : {total}")
print(f"Time elapsed  : {elapsed:.2f} s")
print(f"Throughput    : {total / elapsed:.2f} edges/sec")

conn.close()