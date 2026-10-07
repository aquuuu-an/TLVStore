import csv
import time
import threading
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======================
# Neo4j 配置
# ======================
NEO4J_URI = "bolt://localhost:17687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

EDGES_FILE = "/home/luwenqi/data/influx-cpu-only-10-edges.csv"

BATCH_SIZE = 10_000
NUM_WORKERS = 8

# ======================
# 全局统计（线程安全）
# ======================
lock = threading.Lock()
total_edges_written = 0
total_batches_done = 0
start_time = time.perf_counter()
stop_monitor = False

# ======================
# Neo4j Driver
# ======================
driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD),
    max_connection_pool_size=NUM_WORKERS + 2
)

# ======================
# 写入函数
# ======================
def insert_edges(tx, rows):
    tx.run("""
    UNWIND $rows AS row
    MATCH (s:Vertex {id: row.source})
    MATCH (d:Vertex {id: row.destination})
    CREATE (s)-[:CALL {
        timestamp: row.timestamp,
        region: row.region,
        datacenter: row.datacenter,
        rack: row.rack,
        service: row.service,
        usage_user: row.usage_user,
        usage_system: row.usage_system,
        usage_idle: row.usage_idle
    }]->(d)
    """, rows=rows)

# ======================
# CSV batch reader
# ======================
def edge_batch_reader(csv_path, batch_size):
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append({
                "source": row["source"],
                "destination": row["destination"],
                "timestamp": int(row["timestamp"]),
                "region": row["region"],
                "datacenter": row["datacenter"],
                "rack": row["rack"],
                "service": row["service"],
                "usage_user": int(row["usage_user"]),
                "usage_system": int(row["usage_system"]),
                "usage_idle": int(row["usage_idle"]),
            })
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

# ======================
# 单 batch 写入
# ======================
def write_edge_batch(batch_id, batch):
    global total_edges_written, total_batches_done

    t0 = time.perf_counter()
    with driver.session() as session:
        session.execute_write(insert_edges, batch)
    dt = time.perf_counter() - t0

    with lock:
        total_edges_written += len(batch)
        total_batches_done += 1

    return batch_id, len(batch), dt

# ======================
# 监控线程：每秒打印一次
# ======================
def monitor():
    while not stop_monitor:
        time.sleep(1)
        with lock:
            elapsed = time.perf_counter() - start_time
            if elapsed == 0:
                continue
            speed = total_edges_written / elapsed
            print(
                f"[PROGRESS] "
                f"edges={total_edges_written:,} "
                f"batches={total_batches_done} "
                f"elapsed={elapsed:6.1f}s "
                f"speed={speed:,.0f} edges/s"
            )

# ======================
# 主流程
# ======================
print("===== Neo4j 8-thread Edge Ingest (with progress log) =====")

monitor_thread = threading.Thread(target=monitor, daemon=True)
monitor_thread.start()

futures = []
batch_id = 0

with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    for batch in edge_batch_reader(EDGES_FILE, BATCH_SIZE):
        batch_id += 1
        futures.append(
            executor.submit(write_edge_batch, batch_id, batch)
        )

    for fut in as_completed(futures):
        bid, size, dt = fut.result()
        print(
            f"[BATCH DONE] "
            f"batch={bid:5d} "
            f"size={size:6d} "
            f"time={dt:6.3f}s "
            f"tp={size/dt:8.0f} edges/s"
        )

# ======================
# 结束
# ======================
stop_monitor = True
elapsed = time.perf_counter() - start_time

print("\n===== FINAL RESULT =====")
print(f"Total edges written : {total_edges_written:,}")
print(f"Total time          : {elapsed:.2f}s")
print(f"Average throughput  : {total_edges_written/elapsed:,.0f} edges/s")

driver.close()


