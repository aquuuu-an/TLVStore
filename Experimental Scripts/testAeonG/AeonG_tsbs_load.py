# import time
# import argparse
# import csv
# import os
# import mgclient

# # =====================================================
# # 基本配置（Memgraph）
# # =====================================================
# HOST = "127.0.0.1"
# PORT = 7687
# USER = ""        # Memgraph 默认无认证，改为空字符串
# PASSWORD = ""    # Memgraph 默认无认证，改为空字符串

# BATCH_SIZE = 1000  # 单事务批量大小

# # 1. 节点
# UPSERT_NODE = """
# UNWIND $batch AS row
# CREATE (n:Node {id: row.id})
# SET
#     n.service_environment = row.service_environment,
# """

# # 2. 边
# CREATE_EDGE = """
# UNWIND $batch AS row
# MATCH (a:Node {id: row.source}), (b:Node {id: row.destination})
# CREATE (a)-[r:CONNECTED {ts: toInteger(row.timestamp)}]->(b)
# SET
#     r.service = row.service,
#     r.usage_guest = toInteger(row.usage_guest),
# """

# def load_csv_in_batches(conn, file_path, cypher):
#     if not os.path.exists(file_path):
#         print(f"[Skip] {file_path} not found.")
#         return 0

#     total = 0
#     batch = []

#     try:
#         with open(file_path, "r") as f:
#             reader = csv.DictReader(f)
            
#             # 打印CSV文件的列名，用于调试
#             print(f"    CSV columns: {reader.fieldnames}")

#             for row in reader:
#                 batch.append(row)
#                 if len(batch) >= BATCH_SIZE:
#                     try:
#                         cursor = conn.cursor()
#                         cursor.execute(cypher, {"batch": batch})
#                         conn.commit()
#                         print(f"    Progress: {total + len(batch)} rows", end="\r")
#                     except Exception as e:
#                         print(f"\n❌ 批量写入失败: {e}")
#                         # 打印更详细的错误信息
#                         import traceback
#                         traceback.print_exc()
#                     total += len(batch)
#                     batch.clear()

#             if batch:
#                 try:
#                     cursor = conn.cursor()
#                     cursor.execute(cypher, {"batch": batch})
#                     conn.commit()
#                 except Exception as e:
#                     print(f"\n❌ 批量写入失败: {e}")
#                     import traceback
#                     traceback.print_exc()
#                 total += len(batch)
                
#         print(f"    Completed: {total} rows")
#     except Exception as e:
#         print(f"❌ 文件读取失败: {e}")
#         import traceback
#         traceback.print_exc()
#         return 0

#     return total

# # =====================================================
# # 主流程
# # =====================================================
# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--scale", type=int, required=True)
#     args = parser.parse_args()

#     node_file = f"/home/luwenqi/data/influx-cpu-only-{args.scale}-nodes.csv"
#     edge_file = f"/home/luwenqi/data/influx-cpu-only-{args.scale}-edges.csv"
#     # 使用正确的连接参数
#     conn = mgclient.connect(
#         host=HOST,
#         port=PORT,
#         username=USER,
#         password=PASSWORD
#     )
    
#     print(f"\n===== Scale {args.scale} (Memgraph) Start =====")
#     print(f"Connected to Memgraph at {HOST}:{PORT}")
    
#     start = time.time()

#     # 1. 写节点
#     print("[1/2] Loading nodes...")
#     n_cnt = load_csv_in_batches(conn, node_file, UPSERT_NODE)
#     print(f"    Nodes loaded: {n_cnt}")

#     # 2. 写边
#     print("[2/2] Loading edges...")
#     e_cnt = load_csv_in_batches(conn, edge_file, CREATE_EDGE)
#     print(f"    Edges loaded: {e_cnt}")

#     elapsed = time.time() - start
#     total = n_cnt + e_cnt

#     print("\n===== Finished =====")
#     print(f"Total elements : {total}")
#     print(f"Elapsed time   : {elapsed:.2f} s")
#     print(f"Throughput     : {total / elapsed:.2f} ops/s")

#     conn.close()

# # =====================================================
# if __name__ == "__main__":
#     main()

import time
import argparse
import csv
import os
import mgclient

# =====================================================
# 配置
# =====================================================
HOST = "127.0.0.1"
PORT = 7687
USER = ""
PASSWORD = ""

BATCH_SIZE = 20000   

# =====================================================
# 1️⃣ 节点写入
# =====================================================
# CREATE_NODE = """
# UNWIND $batch AS row
# CREATE (n:Node {id: row.id})
# SET n.service_environment =
#     CASE WHEN row.service_environment <> ""
#     THEN row.service_environment
#     ELSE NULL END
# """

CREATE_NODE = """
UNWIND $batch AS row
CREATE (n:Node {id: row.id})
SET
    n.service_environment = row.service_environment,
    n.os = row.os,
    n.arch = row.arch,
    n.team = row.team,
    n.service_version = row.service_version
"""

# # =====================================================
# # 2️⃣ 高性能建边（使用 internal id）
# # =====================================================
CREATE_EDGE_FAST = """
UNWIND $batch AS row
MATCH (a) WHERE id(a) = row.a
MATCH (b) WHERE id(b) = row.b
CREATE (a)-[:CONNECTED {
    transaction_ts: row.ts,
    service: row.service,
    usage_guest: row.usage_guest,
    usage_guest_nice: row.usage_guest_nice,
    usage_idle: row.usage_idle,
    usage_iowait:  row.usage_iowait,
    usage_irq: row.usage_irq,
    usage_nice: row.usage_nice,
    usage_softirq: row.usage_softirq,
    usage_steal: row.usage_steal,
    usage_system: row.usage_system,
    usage_user: row.usage_user
}]->(b)
"""

# CREATE_EDGE_FAST = """
# UNWIND $batch AS row
# MATCH (a) WHERE id(a) = row.a
# MATCH (b) WHERE id(b) = row.b
# CREATE (a)-[r:CONNECTED {TT_FROM: row.ts, TT_TO:row.ts2}]->(b)
# SET
#     r.service = row.service,
#     r.usage_guest = row.usage_guest,
#     r.usage_guest_nice = row.usage_guest_nice,
#     r.usage_idle = row.usage_idle,
#     r.usage_iowait = row.usage_iowait,
#     r.usage_irq = row.usage_irq,
#     r.usage_nice = row.usage_nice,
#     r.usage_softirq = row.usage_softirq,
#     r.usage_steal = row.usage_steal,
#     r.usage_system = row.usage_system,
#     r.usage_user = row.usage_user
# """

# =====================================================
# 批量加载 CSV
# =====================================================
def load_nodes(conn, file_path):
    total = 0
    batch = []

    with open(file_path, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                cursor = conn.cursor()
                cursor.execute(CREATE_NODE, {"batch": batch})
                conn.commit()
                total += len(batch)
                batch.clear()

        if batch:
            cursor = conn.cursor()
            cursor.execute(CREATE_NODE, {"batch": batch})
            conn.commit()
            total += len(batch)

    return total


def build_id_map(conn):
    print("Loading Node.id -> internal id map...")

    cursor = conn.cursor()
    cursor.execute("MATCH (n:Node) RETURN n.id, id(n)")

    id_map = {}
    for record in cursor.fetchall():
        id_map[record[0]] = record[1]

    print(f"Loaded {len(id_map)} nodes into memory")
    return id_map


def load_edges_fast(conn, file_path, id_map):
    total = 0
    batch = []

    with open(file_path, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:

            # 转换为 internal id
            a_id = id_map.get(row["source"])
            b_id = id_map.get(row["destination"])

            if a_id is None or b_id is None:
                continue

            batch.append({
                "a": a_id,
                "b": b_id,
                "ts":int(row["timestamp"]),
                "service": row["service"] if row["service"] else None,
                "usage_guest": int(row["usage_guest"]) if row["usage_guest"] else None,
                "usage_guest_nice": int(row["usage_guest_nice"]) if row["usage_guest_nice"] else None,
                "usage_idle":   int(row["usage_idle"]) if row["usage_idle"] else None,
                "usage_iowait":    int(row["usage_iowait"]) if row["usage_iowait"] else None,
                "usage_irq":   int(row["usage_irq"]) if row["usage_irq"] else None,
                "usage_nice":   int(row["usage_nice"]) if row["usage_nice"] else None,
                "usage_softirq":   int(row["usage_softirq"]) if row["usage_softirq"] else None,
                "usage_steal":  int(row["usage_steal"]) if row["usage_steal"] else None,
                "usage_system":  int(row["usage_system"]) if row["usage_system"] else None,
                "usage_user":  int(row["usage_user"]) if row["usage_user"] else None
            })

            if len(batch) >= BATCH_SIZE:
                cursor = conn.cursor()
                cursor.execute(CREATE_EDGE_FAST, {"batch": batch})
                conn.commit()
                total += len(batch)
                batch.clear()
                print(f"Progress: {total}", end="\r")

        if batch:
            cursor = conn.cursor()
            cursor.execute(CREATE_EDGE_FAST, {"batch": batch})
            conn.commit()
            total += len(batch)

    print()
    return total


# =====================================================
# 主程序
# =====================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, required=True)
    args = parser.parse_args()

    node_file = f"/home/luwenqi/data/influx-cpu-only-{args.scale}-nodes.csv"
    edge_file = f"/home/luwenqi/data/influx-cpu-only-{args.scale}-edges.csv"

    conn = mgclient.connect(
        host=HOST,
        port=PORT,
        username=USER,
        password=PASSWORD
    )

    print(f"\n===== Scale {args.scale} (FAST MODE) =====")

    start = time.time()

    # 1️⃣ 写节点
    print("[1/3] Loading nodes...")
    n_cnt = load_nodes(conn, node_file)
    print(f"Nodes loaded: {n_cnt}")

    # 2️⃣ 构建 internal id 映射
    print("[2/3] Building id map...")
    id_map = build_id_map(conn)

    # 3️⃣ 高性能写边
    print("[3/3] Loading edges (FAST)...")
    e_cnt = load_edges_fast(conn, edge_file, id_map)
    print(f"Edges loaded: {e_cnt}")

    elapsed = time.time() - start

    print("\n===== Finished =====")
    print(f"Total elements : {n_cnt + e_cnt}")
    print(f"Elapsed time   : {elapsed:.2f} s")
    print(f"Throughput     : {(n_cnt + e_cnt) / elapsed:.2f} ops/s")

    conn.close()


if __name__ == "__main__":
    main()