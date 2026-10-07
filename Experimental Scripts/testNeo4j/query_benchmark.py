# import time, os, argparse, json
# from neo4j import GraphDatabase

# # 配置
# QUERY_DIR = "queries_neo4j"
# LOG_BASE  = "queryLogs"
# URI, USER, PASS = "bolt://localhost:17687", "neo4j", "12345678"

# def run_query(session, cypher):
#     start = time.perf_counter()
#     try:
#         result = session.run(cypher)
#         records = list(result) # 强制拉取数据以计算真实时延
#         rows = len(records)
#         latency = (time.perf_counter() - start) * 1000
#         status = "SUCCESS" if rows > 0 else "EMPTY"
#         content = json.dumps([dict(r) for r in records[:1]]) # 仅保留一行样例
#         return status, rows, latency, content
#     except Exception as e:
#         latency = (time.perf_counter() - start) * 1000
#         return "FAILED", 0, latency, str(e)

# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--scale", type=int, required=True)
#     args = parser.parse_args()
    
#     log_dir = os.path.join(LOG_BASE, str(args.scale))
#     os.makedirs(log_dir, exist_ok=True)
    
#     driver = GraphDatabase.driver(URI, auth=(USER, PASS))
#     with driver.session() as session:
#         for fname in sorted(os.listdir(QUERY_DIR)):
#             if not fname.endswith(".cypher"): continue
            
#             q_type = fname.replace(".cypher", "")
#             out_path = os.path.join(log_dir, f"{q_type}.txt")
#             lats = []
#             c_s, c_e, c_f = 0, 0, 0
            
#             with open(out_path, "w") as f:
#                 f.write(f"Query: {q_type} | Scale: {args.scale}M\n" + "="*100 + "\n")
#                 f.write(f"{'ID':<5} | {'Status':<8} | {'Latency':<10} | {'Rows':<8} | {'Info'}\n" + "-"*100 + "\n")
                
#                 with open(os.path.join(QUERY_DIR, fname)) as qf:
#                     for i, line in enumerate(qf):
#                         if not line.strip(): continue
#                         status, rows, lat, info = run_query(session, line.strip())
#                         f.write(f"{i:<5} | {status:<8} | {lat:>7.2f}ms | {rows:<8} | {info[:50]}\n")
#                         if status == "FAILED": c_f += 1
#                         else:
#                             lats.append(lat)
#                             if status == "SUCCESS": c_s += 1
#                             else: c_e += 1
                
#                 avg = sum(lats)/len(lats) if lats else 0
#                 f.write(f"-"*100 + f"\nSummary: Avg {avg:.2f}ms | Success {c_s} | Empty {c_e} | Fail {c_f}\n")
#             print(f"Scale {args.scale}M | Query {q_type} | Avg: {avg:.2f}ms")
#     driver.close()

# if __name__ == "__main__":
#     main()

import time, os, argparse, json
import requests

# 配置
QUERY_DIR = "queries_neo4j"
LOG_BASE  = "queryLogs_http"
# HTTP 端口默认为 7474, 注意：新版本 Neo4j 默认数据库名为 neo4j
HTTP_URL = "http://localhost:17474/db/neo4j/tx/commit" 
USER, PASS = "neo4j", "12345678"

def run_query_http(session, cypher):
    """使用 HTTP POST 发送 Cypher 查询"""
    payload = {
        "statements": [
            {
                "statement": cypher,
                "resultDataContents": ["row"]
            }
        ]
    }
    
    start = time.perf_counter()
    try:
        response = session.post(
            HTTP_URL, 
            json=payload, 
            auth=(USER, PASS),
            timeout=30
        )
        response.raise_for_status()
        res_json = response.json()
        
        if res_json.get("errors"):
            raise Exception(res_json["errors"][0]["message"])
            
        results = res_json["results"][0]["data"]
        rows = len(results)
        
        # 计时点：在解析完 JSON 后结束，确保包含序列化损耗
        latency = (time.perf_counter() - start) * 1000
        status = "SUCCESS" if rows > 0 else "EMPTY"
        content = json.dumps(results[0]["row"]) if rows > 0 else ""
        return status, rows, latency, content
        
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        return "FAILED", 0, latency, str(e)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, required=True)
    args = parser.parse_args()
    
    log_dir = os.path.join(LOG_BASE, str(args.scale))
    os.makedirs(log_dir, exist_ok=True)
    
    # Session 保持 Keep-Alive 减少 TCP 握手开销
    with requests.Session() as session:
        for fname in sorted(os.listdir(QUERY_DIR)):
            if not fname.endswith(".cypher"): continue
            
            q_type = fname.replace(".cypher", "")
            out_path = os.path.join(log_dir, f"{q_type}.txt")
            valid_lats = [] # 仅存储有效查询的时延
            failed_count = 0
            
            with open(out_path, "w") as f:
                f.write(f"Query: {q_type} | Scale: {args.scale}M | Protocol: HTTP\n" + "="*100 + "\n")
                f.write(f"{'ID':<5} | {'Status':<8} | {'Latency':<10} | {'Rows':<8} | {'Info'}\n" + "-"*100 + "\n")
                
                with open(os.path.join(QUERY_DIR, fname)) as qf:
                    lines = [line.strip() for line in qf if line.strip()]
                    
                    for i, line in enumerate(lines):
                        status, rows, lat, info = run_query_http(session, line)
                        
                        # 1. 处理预热 (Warm-up)
                        if i == 0:
                            f.write(f"{i:<5} | WARMUP   | {lat:>7.2f}ms | {rows:<8} | (Skipped from stats)\n")
                            continue

                        # 2. 记录详情
                        f.write(f"{i:<5} | {status:<8} | {lat:>7.2f}ms | {rows:<8} | {info[:50]}\n")
                        
                        # 3. 统计逻辑：只有非失败请求计入平均时延
                        if status == "FAILED":
                            failed_count += 1
                        else:
                            valid_lats.append(lat)
                
                # 计算平均值、中位数、P95
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
                    print(f"Scale {args.scale}M | Query {q_type} | Avg: {avg_lat:.2f}ms | Median: {median_lat:.2f}ms")
                else:
                    print(f"Scale {args.scale}M | Query {q_type} | All queries failed.")

if __name__ == "__main__":
    main()