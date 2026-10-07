# # import time
# # import os
# # import argparse
# # from influxdb import InfluxDBClient

# # # =====================================================
# # # 固定配置
# # # =====================================================
# # QUERY_DIR = "queries_influx"
# # LOG_BASE  = "queryLogs"

# # INFLUX_HOST = "localhost"
# # INFLUX_PORT = 8086
# # INFLUX_DB   = "lwq"

# # def parse_args():
# #     parser = argparse.ArgumentParser(description="InfluxDB query performance benchmark")
# #     parser.add_argument("--scale", type=int, required=True, help="Dataset scale label")
# #     return parser.parse_args()

# # def run_query(client, query):
# #     start = time.time()
# #     try:
# #         result = client.query(query)
# #         # 修复点：ResultSet 使用 get_points() 获取数据迭代器
# #         # 将其转化为列表后计算长度即为行数
# #         points = list(result.get_points())
# #         rows = len(points)
        
# #         end = time.time()
# #         latency = (end - start) * 1000
        
# #         if rows > 0:
# #             return "SUCCESS", rows, latency, "OK"
# #         else:
# #             return "EMPTY", 0, latency, "No data returned"
            
# #     except Exception as e:
# #         end = time.time()
# #         err_msg = str(e).replace('\n', ' ')
# #         return "FAILED", 0, (end - start) * 1000, err_msg

# # def main():
# #     args = parse_args()
    
# #     log_dir = os.path.join(LOG_BASE, str(args.scale))
# #     os.makedirs(log_dir, exist_ok=True)

# #     client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)

# #     for fname in sorted(os.listdir(QUERY_DIR)):
# #         if not fname.endswith(".influxql"):
# #             continue

# #         query_type = fname.replace(".influxql", "")
# #         output_file = os.path.join(log_dir, f"{query_type}.txt")
        
# #         latencies = []
# #         c_success, c_empty, c_failed = 0, 0, 0
        
# #         with open(output_file, "w", encoding="utf-8") as out_f:
# #             out_f.write(f"Query Type: {query_type} | Scale: {args.scale}\n")
# #             out_f.write("=" * 100 + "\n")
# #             # 增加 Error_Message 列
# #             out_f.write(f"{'Query_ID':<10} | {'Status':<10} | {'Latency':<12} | {'Rows':<8} | {'Message/Error'}\n")
# #             out_f.write("-" * 100 + "\n")

# #             with open(os.path.join(QUERY_DIR, fname)) as qf:
# #                 for i, line in enumerate(qf):
# #                     line = line.strip()
# #                     if not line: continue

# #                     status, rows, latency, msg = run_query(client, line)

# #                     # 记录详细状态和错误原因
# #                     out_f.write(f"{i:<10} | {status:<10} | {latency:>8.2f} ms | {rows:<8} | {msg}\n")

# #                     if status == "FAILED":
# #                         c_failed += 1
# #                     else:
# #                         latencies.append(latency)
# #                         if status == "SUCCESS":
# #                             c_success += 1
# #                         else:
# #                             c_empty += 1

# #             avg_latency = sum(latencies) / len(latencies) if latencies else 0
            
# #             out_f.write("-" * 100 + "\n")
# #             out_f.write(f"Summary Statistics:\n")
# #             out_f.write(f"Total Queries:    {c_success + c_empty + c_failed}\n")
# #             out_f.write(f"Success (Data):   {c_success}\n")
# #             out_f.write(f"Success (Empty):  {c_empty}\n")
# #             out_f.write(f"Failed:           {c_failed}\n")
# #             out_f.write(f"Average Latency:  {avg_latency:.3f} ms (Excluding Fails)\n")

# #         print(f"Finished: {query_type:<20} | Avg: {avg_latency:>8.2f} ms | Log: {output_file}")

# # if __name__ == "__main__":
# #     main()

# # import time
# # import os
# # import argparse
# # import json
# # from influxdb import InfluxDBClient

# # # =====================================================
# # # 固定配置
# # # =====================================================
# # QUERY_DIR = "queries_influx"
# # LOG_BASE  = "queryLogs"

# # INFLUX_HOST = "localhost"
# # INFLUX_PORT = 8086
# # INFLUX_DB   = "lwq"

# # def parse_args():
# #     parser = argparse.ArgumentParser(description="InfluxDB query performance benchmark")
# #     parser.add_argument("--scale", type=int, required=True, help="Dataset scale label")
# #     return parser.parse_args()

# # def run_query(client, query):
# #     start = time.time()
# #     try:
# #         result = client.query(query)
# #         raw_data = result.raw
        
# #         # 改进的行数统计逻辑：兼容多种 InfluxDB 响应格式
# #         rows = 0
# #         # 1. 检查是否存在 'results' (标准完整响应)
# #         res_list = raw_data.get('results', [raw_data]) 
        
# #         for res in res_list:
# #             if 'series' in res:
# #                 for s in res['series']:
# #                     rows += len(s.get('values', []))
        
# #         end = time.time()
# #         latency = (end - start) * 1000
        
# #         status = "SUCCESS" if rows > 0 else "EMPTY"
# #         return status, rows, latency, json.dumps(raw_data)
            
# #     except Exception as e:
# #         end = time.time()
# #         err_msg = f"ERROR: {str(e)}"
# #         return "FAILED", 0, (end - start) * 1000, err_msg

# # def main():
# #     args = parse_args()
# #     log_dir = os.path.join(LOG_BASE, str(args.scale))
# #     os.makedirs(log_dir, exist_ok=True)

# #     client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)

# #     for fname in sorted(os.listdir(QUERY_DIR)):
# #         if not fname.endswith(".influxql"):
# #             continue

# #         query_type = fname.replace(".influxql", "")
# #         output_file = os.path.join(log_dir, f"{query_type}.txt")
        
# #         latencies = []
# #         c_success, c_empty, c_failed = 0, 0, 0
        
# #         with open(output_file, "w", encoding="utf-8") as out_f:
# #             out_f.write(f"Query Type: {query_type} | Scale: {args.scale}\n")
# #             out_f.write("=" * 120 + "\n")
# #             out_f.write(f"{'Query_ID':<10} | {'Status':<10} | {'Latency':<12} | {'Rows':<8} | {'Raw Result / Error Message'}\n")
# #             out_f.write("-" * 120 + "\n")

# #             with open(os.path.join(QUERY_DIR, fname)) as qf:
# #                 for i, line in enumerate(qf):
# #                     line = line.strip()
# #                     if not line: continue

# #                     status, rows, latency, content = run_query(client, line)

# #                     out_f.write(f"{i:<10} | {status:<10} | {latency:>8.2f} ms | {rows:<8} | {content}\n")

# #                     if status == "FAILED":
# #                         c_failed += 1
# #                     else:
# #                         latencies.append(latency)
# #                         if status == "SUCCESS":
# #                             c_success += 1
# #                         else:
# #                             c_empty += 1

# #             avg_latency = sum(latencies) / len(latencies) if latencies else 0
            
# #             out_f.write("-" * 120 + "\n")
# #             out_f.write(f"Summary Statistics:\n")
# #             out_f.write(f"Total Queries:    {c_success + c_empty + c_failed}\n")
# #             out_f.write(f"Success (Data):   {c_success}\n")
# #             out_f.write(f"Success (Empty):  {c_empty}\n")
# #             out_f.write(f"Failed:           {c_failed}\n")
# #             out_f.write(f"Average Latency:  {avg_latency:.3f} ms (Excluding Fails)\n")

# #         print(f"Finished: {query_type:<20} | Avg: {avg_latency:>8.2f} ms | Log: {output_file}")

# # if __name__ == "__main__":
# #     main()

import time
import os
import argparse
import json
from influxdb import InfluxDBClient

# =====================================================
# 固定配置
# =====================================================
QUERY_DIR = "queries_influx"
LOG_BASE  = "queryLogs"

INFLUX_HOST = "localhost"
INFLUX_PORT = 8086
INFLUX_DB   = "lwq"

def parse_args():
    parser = argparse.ArgumentParser(description="InfluxDB query performance benchmark")
    parser.add_argument("--scale", type=int, required=True, help="Dataset scale label")
    return parser.parse_args()

def run_query(client, query):
    # 使用 perf_counter 提供更高精度
    start = time.perf_counter()
    try:
        result = client.query(query)
        raw_data = result.raw
        latency = (time.perf_counter() - start) * 1000
        
        rows = 0
        res_list = raw_data.get('results', [raw_data]) 
        for res in res_list:
            if 'series' in res:
                for s in res['series']:
                    rows += len(s.get('values', []))
        
        
        status = "SUCCESS" if rows > 0 else "EMPTY"
        return status, rows, latency, json.dumps(raw_data)
            
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        return "FAILED", 0, latency, f"ERROR: {str(e)}"

def main():
    args = parse_args()
    log_dir = os.path.join(LOG_BASE, str(args.scale))
    os.makedirs(log_dir, exist_ok=True)

    # InfluxDBClient 默认底层使用 requests.Session()，保持连接复用
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)

    for fname in sorted(os.listdir(QUERY_DIR)):
        if not fname.endswith(".influxql"):
            continue

        query_type = fname.replace(".influxql", "")
        output_file = os.path.join(log_dir, f"{query_type}.txt")
        
        valid_lats = []
        c_success, c_empty, c_failed = 0, 0, 0
        
        with open(output_file, "w", encoding="utf-8") as out_f:
            out_f.write(f"Query Type: {query_type} | Scale: {args.scale} | Protocol: HTTP\n")
            out_f.write("=" * 120 + "\n")
            out_f.write(f"{'Query_ID':<10} | {'Status':<10} | {'Latency':<12} | {'Rows':<8} | {'Info (Truncated)'}\n")
            out_f.write("-" * 120 + "\n")

            with open(os.path.join(QUERY_DIR, fname)) as qf:
                lines = [line.strip() for line in qf if line.strip()]
                for i, line in enumerate(lines):
                    status, rows, latency, content = run_query(client, line)

                    # --- 修改 1: 预热逻辑 (跳过第一条) ---
                    if i == 0:
                        out_f.write(f"{i:<10} | WARMUP     | {latency:>8.2f} ms | {rows:<8} | (Skipped from stats)\n")
                        continue

                    # --- 修改 2: 记录详情 (截断输出) ---
                    out_f.write(f"{i:<10} | {status:<10} | {latency:>8.2f} ms | {rows:<8} | {content[:100]}\n")

                    if status == "FAILED":
                        c_failed += 1
                    else:
                        valid_lats.append(latency)
                        if status == "SUCCESS":
                            c_success += 1
                        else:
                            c_empty += 1

            # --- 修改 3: 增强统计指标 ---
            if valid_lats:
                valid_lats.sort()
                avg_latency = sum(valid_lats) / len(valid_lats)
                median_latency = valid_lats[len(valid_lats) // 2]
                p95_latency = valid_lats[int(len(valid_lats) * 0.95)]
                
                out_f.write("-" * 120 + "\n")
                out_f.write(f"Summary Statistics (Excluding Warmup & Fails):\n")
                out_f.write(f"Total Effective:  {len(valid_lats)}\n")
                out_f.write(f"Average Latency:  {avg_latency:.3f} ms\n")
                out_f.write(f"Median Latency:   {median_latency:.3f} ms\n")
                out_f.write(f"P95 Latency:      {p95_latency:.3f} ms\n")
                out_f.write(f"Failed Count:     {c_failed}\n")
            else:
                avg_latency = 0
                out_f.write("\nNo valid query results to summarize.\n")

        print(f"Finished: {query_type:<20} | Avg: {avg_latency:>8.2f} ms | Median: {median_latency:>8.2f} ms")

if __name__ == "__main__":
    main()