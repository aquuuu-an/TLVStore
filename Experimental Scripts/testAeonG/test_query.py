import mgclient

# 1️⃣ 连接 Memgraph
conn = mgclient.connect(
    host="127.0.0.1",
    port=7687,
    username="",    # 如果没设置密码
    password=""
)
cursor = conn.cursor()

# 2️⃣ 查询节点数量
cursor.execute("MATCH (n) RETURN count(n)")
count = cursor.fetchone()[0]
print(f"Total nodes: {count}")

# 3️⃣ 查询边信息
cursor.execute("""
MATCH (a)-[r:CONNECTED]->(b)
RETURN a.id, b.id, r.ts
LIMIT 10
""")
for row in cursor.fetchall():
    print(row)

# 4️⃣ 关闭连接
conn.close()