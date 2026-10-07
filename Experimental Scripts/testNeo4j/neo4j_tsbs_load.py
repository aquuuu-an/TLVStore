# import time
# import argparse
# import csv
# import os
# from neo4j import GraphDatabase
# from concurrent.futures import ThreadPoolExecutor

# # 配置参数
# URI, USER, PASS = "bolt://localhost:17687", "neo4j", "12345678"
# BATCH_SIZE = 10000 
# THREADS = 8       # 增加并发数以提高吞吐

# # Cypher 模板
# UPSERT_NODE = """
# UNWIND $batch AS row
# MERGE (n:Node {id: row.id})
# SET
#     n.service_environment = row.service_environment,
#     n.os = row.os,
#     n.arch = row.arch,
#     n.team = row.team,
#     n.service_version = row.service_version
# """

# UPSERT_EDGE = """
# UNWIND $batch AS row
# MATCH (a:Node {id: row.source})
# MATCH (b:Node {id: row.destination})
# MERGE (a)-[r:CONNECTED {ts: toInteger(row.timestamp)}]->(b)
# SET
#     r.region = row.region,
#     r.datacenter = row.datacenter,
#     r.rack = row.rack,
#     r.service = row.service,
#     r.usage_guest = toInteger(row.usage_guest),
#     r.usage_guest_nice = toInteger(row.usage_guest_nice),
#     r.usage_idle = toInteger(row.usage_idle),
#     r.usage_iowait = toInteger(row.usage_iowait),
#     r.usage_irq = toInteger(row.usage_irq),
#     r.usage_nice = toInteger(row.usage_nice),
#     r.usage_softirq = toInteger(row.usage_softirq),
#     r.usage_steal = toInteger(row.usage_steal),
#     r.usage_system = toInteger(row.usage_system),
#     r.usage_user = toInteger(row.usage_user)
# """

# def write_batch(driver, cypher, batch):
#     with driver.session() as session:
#         session.execute_write(lambda tx: tx.run(cypher, batch=batch))

# def process_file(driver, file_path, cypher):
#     if not os.path.exists(file_path):
#         print(f"Skipping: {file_path} not found.")
#         return 0
    
#     count = 0
#     with ThreadPoolExecutor(max_workers=THREADS) as executor:
#         batch = []
#         with open(file_path, 'r') as f:
#             reader = csv.DictReader(f)
#             for row in reader:
#                 batch.append(row)
#                 if len(batch) >= BATCH_SIZE:
#                     executor.submit(write_batch, driver, cypher, batch.copy())
#                     count += len(batch)
#                     batch = []
#             if batch:
#                 executor.submit(write_batch, driver, cypher, batch)
#                 count += len(batch)
#     return count

# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--scale", type=int, required=True)
#     args = parser.parse_args()

#     # 构造文件路径
#     node_file = f"/home/luwenqi/data/influx-cpu-only-{args.scale}-nodes.csv"
#     edge_file = f"/home/luwenqi/data/influx-cpu-only-{args.scale}-edges.csv"

#     driver = GraphDatabase.driver(URI, auth=(USER, PASS))
    
#     print(f"--- Scale {args.scale} Start ---")
#     start_time = time.time()

#     # 1. 先写节点 (必须先存在节点才能连边)
#     print(f"Loading Nodes from {node_file}...")
#     n_count = process_file(driver, node_file, UPSERT_NODE)

#     # 2. 再写边
#     print(f"Loading Edges from {edge_file}...")
#     e_count = process_file(driver, edge_file, UPSERT_EDGE)

#     duration = time.time() - start_time
#     total = n_count + e_count
#     print(f"Finished: {total} elements in {duration:.2f}s | TPS: {total/duration:.2f}")
    
#     driver.close()

# if __name__ == "__main__":
#     main()

import time
import argparse
import csv
import os
from neo4j import GraphDatabase

# =====================================================
# 基本配置
# =====================================================
URI, USER, PASS = "bolt://localhost:17687", "neo4j", "12345678"

BATCH_SIZE = 10000     # 控制单事务内存
# ⚠️ 不要再加线程

# =====================================================
# Cypher 模板
# =====================================================

# 1. 节点：仍然用 MERGE（保证唯一性）
UPSERT_NODE = """
UNWIND $batch AS row
MERGE (n:Node {id: row.id})
SET
    n.service_environment = row.service_environment,
    n.os = row.os,
    n.arch = row.arch,
    n.team = row.team,
    n.service_version = row.service_version
"""

# 2. 边：用 CREATE，避免 MERGE 的全图查找 + 锁
CREATE_EDGE = """
UNWIND $batch AS row
MATCH (a:Node {id: row.source}), (b:Node {id: row.destination})
CREATE (a)-[r:CONNECTED {ts: toInteger(row.timestamp)}]->(b)
SET
    r.region = row.region,
    r.datacenter = row.datacenter,
    r.rack = row.rack,
    r.service = row.service,
    r.usage_guest = toInteger(row.usage_guest),
    r.usage_guest_nice = toInteger(row.usage_guest_nice),
    r.usage_idle = toInteger(row.usage_idle),
    r.usage_iowait = toInteger(row.usage_iowait),
    r.usage_irq = toInteger(row.usage_irq),
    r.usage_nice = toInteger(row.usage_nice),
    r.usage_softirq = toInteger(row.usage_softirq),
    r.usage_steal = toInteger(row.usage_steal),
    r.usage_system = toInteger(row.usage_system),
    r.usage_user = toInteger(row.usage_user)
"""

# =====================================================
# 批量写入函数（单线程、同步）
# =====================================================
def load_csv_in_batches(driver, file_path, cypher):
    if not os.path.exists(file_path):
        print(f"[Skip] {file_path} not found.")
        return 0

    total = 0
    batch = []

    with open(file_path, "r") as f, driver.session() as session:
        reader = csv.DictReader(f)

        for row in reader:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                session.execute_write(
                    lambda tx: tx.run(cypher, batch=batch)
                )
                total += len(batch)
                batch.clear()

        if batch:
            session.execute_write(
                lambda tx: tx.run(cypher, batch=batch)
            )
            total += len(batch)

    return total

# =====================================================
# 主流程
# =====================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, required=True)
    args = parser.parse_args()

    node_file = f"/home/luwenqi/data/influx-cpu-only-{args.scale}-nodes.csv"
    edge_file = f"/home/luwenqi/data/influx-cpu-only-{args.scale}-edges.csv"

    driver = GraphDatabase.driver(
        URI,
        auth=(USER, PASS),
        max_connection_pool_size=4   # 明确限制连接池
    )

    print(f"\n===== Scale {args.scale} Start =====")
    start = time.time()

    # 1. 写节点
    print("[1/2] Loading nodes...")
    n_cnt = load_csv_in_batches(driver, node_file, UPSERT_NODE)
    print(f"    Nodes loaded: {n_cnt}")

    # 2. 写边
    print("[2/2] Loading edges...")
    e_cnt = load_csv_in_batches(driver, edge_file, CREATE_EDGE)
    print(f"    Edges loaded: {e_cnt}")

    elapsed = time.time() - start
    total = n_cnt + e_cnt

    print("\n===== Finished =====")
    print(f"Total elements : {total}")
    print(f"Elapsed time   : {elapsed:.2f} s")
    print(f"Throughput    : {total / elapsed:.2f} ops/s")

    driver.close()

# =====================================================
if __name__ == "__main__":
    main()
