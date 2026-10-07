import time, os, argparse, json, traceback
import mgclient

# =====================================================
# 配置（Memgraph Bolt）
# =====================================================
QUERY_DIR = "queries_aeong"
LOG_BASE  = "queryLogs_mgclient_memgraph"
HOST      = "127.0.0.1"
PORT      = 7687
USER      = ""   # 默认无认证
PASSWORD  = ""
RETRY_COUNT = 3  # 遇到异常自动重试次数

# =====================================================
# 执行查询
# =====================================================
def run_query_mgclient(conn, cypher):
    """使用 mgclient 执行 Cypher 查询，并自动重试"""
    for attempt in range(RETRY_COUNT):
        start = time.perf_counter()
        try:
            cursor = conn.cursor()
            cursor.execute(cypher)
            # 获取结果
            try:
                results = cursor.fetchall()
                rows = len(results)
            except mgclient.InterfaceError:
                # 如果查询没有返回数据
                results = []
                rows = 0
            latency = (time.perf_counter() - start) * 1000
            status = "SUCCESS" if rows > 0 else "EMPTY"
            content = results
            return status, rows, latency, content
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            tb = traceback.format_exc()
            if attempt < RETRY_COUNT - 1:
                time.sleep(1)
                continue
            return "FAILED", 0, latency, f"{str(e)}\n{tb}"

# =====================================================
# 主流程
# =====================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, required=True)
    args = parser.parse_args()

    log_dir = os.path.join(LOG_BASE, str(args.scale))
    os.makedirs(log_dir, exist_ok=True)

    failed_log_dir = os.path.join(log_dir, "failed")
    os.makedirs(failed_log_dir, exist_ok=True)

    # 连接 Memgraph
    conn = mgclient.connect(
        host=HOST,
        port=PORT,
        username=USER,
        password=PASSWORD
    )

    for fname in sorted(os.listdir(QUERY_DIR)):
        if not fname.endswith(".cypher"):
            continue

        q_type = fname.replace(".cypher", "")
        out_path = os.path.join(log_dir, f"{q_type}.txt")
        failed_path = os.path.join(failed_log_dir, f"{q_type}_failed.log")

        valid_lats = []
        failed_count = 0

        with open(out_path, "w") as f, open(failed_path, "w") as ff:
            f.write(
                f"Query: {q_type} | Scale: {args.scale}M | Engine: Memgraph | Protocol: mgclient\n"
                + "=" * 100 + "\n"
            )
            f.write(
                f"{'ID':<5} | {'Status':<8} | {'Latency':<10} | {'Rows':<8} | {'Info'}\n"
                + "-" * 100 + "\n"
            )

            with open(os.path.join(QUERY_DIR, fname)) as qf:
                lines = [line.strip() for line in qf if line.strip()]

                for i, line in enumerate(lines):
                    status, rows, lat, info = run_query_mgclient(conn, line)

                    # 1️⃣ Warm-up
                    if i == 0:
                        f.write(
                            f"{i:<5} | WARMUP   | {lat:>7.2f}ms | {rows:<8} | (Skipped from stats)\n"
                        )
                        continue

                    # 2️⃣ 记录到主日志
                    if status == "FAILED":
                        f.write(f"{i:<5} | {status:<8} | {lat:>7.2f}ms | {rows:<8} | See failed log\n")
                        ff.write(f"ID {i} | Latency {lat:.2f}ms | Exception:\n{info}\n{'-'*80}\n")
                        failed_count += 1
                    else:
                        f.write(f"{i:<5} | {status:<8} | {lat:>7.2f}ms | {rows:<8} | {info[:200]}\n")
                        valid_lats.append(lat)

            # 3️⃣ 汇总统计
            if valid_lats:
                valid_lats.sort()
                avg_lat = sum(valid_lats) / len(valid_lats)
                median_lat = valid_lats[len(valid_lats) // 2]
                p95_lat = valid_lats[int(len(valid_lats) * 0.95)]

                summary = (
                    f"{'-'*100}\n"
                    f"Summary (Excluding Warmup & Fails):\n"
                    f"  - Count  : {len(valid_lats)}\n"
                    f"  - Average: {avg_lat:.2f} ms\n"
                    f"  - Median : {median_lat:.2f} ms\n"
                    f"  - P95    : {p95_lat:.2f} ms\n"
                    f"  - Failed : {failed_count}\n"
                )
                f.write(summary)
                print(
                    f"Scale {args.scale}M | Query {q_type} | "
                    f"Avg: {avg_lat:.2f}ms | Median: {median_lat:.2f}ms | Failed: {failed_count}"
                )
            else:
                print(
                    f"Scale {args.scale}M | Query {q_type} | All queries failed."
                )

    conn.close()

# =====================================================
if __name__ == "__main__":
    main()