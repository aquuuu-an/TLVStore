# import csv
# import os

# # ======================
# # 规模列表：70,80,...,120
# # ======================
# SIZES = list(range(70, 121, 10))

# INPUT_DIR = "/home/luwenqi/influxdb-final-test-dir/data/cpu-only"
# OUTPUT_DIR = "/home/luwenqi/data"

# # ======================
# # 节点静态属性（不随时间变化）
# # ======================
# SERVICE_NODE_KEYS = [
#     "service_environment",
#     "os",
#     "arch",
# ]

# HOST_NODE_KEYS = [
#     "team",
#     "service_version",
# ]

# # ======================
# # 边标签属性（结构属性）
# # ======================
# EDGE_TAG_KEYS = [
#     "region",
#     "datacenter",
#     "rack",
#     "service",
# ]

# # ======================
# # 解析 Influx 行
# # ======================
# def parse_line(line: str):
#     meta, rest = line.strip().split(" ", 1)
#     fields_part, ts = rest.rsplit(" ", 1)

#     _, tags_part = meta.split(",", 1)
#     tag_kv = dict(kv.split("=", 1) for kv in tags_part.split(","))

#     field_kv = {}
#     for kv in fields_part.split(","):
#         k, v = kv.split("=")
#         field_kv[k] = int(v.rstrip("i"))

#     return tag_kv, field_kv, int(ts)

# # ======================
# # 主流程
# # ======================
# for size in SIZES:
#     print(f"\n🚀 Processing size = {size}")

#     INPUT_FILE = os.path.join(INPUT_DIR, f"influx-cpu-only-{size}")
#     NODES_FILE = os.path.join(OUTPUT_DIR, f"influx-cpu-only-{size}-nodes.csv")
#     EDGES_FILE = os.path.join(OUTPUT_DIR, f"influx-cpu-only-{size}-edges.csv")

#     nodes = {}   # id -> node dict
#     edges = []   # append-only

#     with open(INPUT_FILE, "r") as f:
#         for line in f:
#             if not line.strip():
#                 continue

#             tag_kv, field_kv, ts = parse_line(line)

#             raw_src = tag_kv["source"]
#             raw_dst = tag_kv["destination"]

#             # ======================
#             # Service Node（源节点）
#             # ======================
#             src_id = f"src_{raw_src}"
#             if src_id not in nodes:
#                 node = {
#                     "id": src_id,
#                     # "type": "service",
#                 }
#                 for k in SERVICE_NODE_KEYS:
#                     node[k] = tag_kv[k]
#                 for k in HOST_NODE_KEYS:
#                     node[k] = ""
#                 nodes[src_id] = node

#             # ======================
#             # Host Node（目的节点）
#             # ======================
#             dst_id = f"dst_{raw_dst}"
#             if dst_id not in nodes:
#                 node = {
#                     "id": dst_id,
#                     # "type": "host",
#                 }
#                 for k in HOST_NODE_KEYS:
#                     node[k] = tag_kv[k]
#                 for k in SERVICE_NODE_KEYS:
#                     node[k] = ""
#                 nodes[dst_id] = node

#             # ======================
#             # Edge Snapshot（时间快照）
#             # ======================
#             edge = {
#                 "source": src_id,
#                 "destination": dst_id,
#                 "timestamp": ts,
#             }

#             for k in EDGE_TAG_KEYS:
#                 edge[k] = tag_kv[k]

#             edge.update(field_kv)
#             edges.append(edge)

#     # ======================
#     # 写 nodes.csv
#     # ======================
#     node_fieldnames = (
#         ["id", "type"]
#         + SERVICE_NODE_KEYS
#         + HOST_NODE_KEYS
#     )

#     with open(NODES_FILE, "w", newline="") as f:
#         writer = csv.DictWriter(f, fieldnames=node_fieldnames)
#         writer.writeheader()
#         for node in nodes.values():
#             writer.writerow(node)

#     # ======================
#     # 写 edges.csv
#     # ======================
#     usage_fields = sorted(
#         k for k in edges[0].keys() if k.startswith("usage_")
#     )

#     edge_fieldnames = (
#         ["source", "destination", "timestamp"]
#         + EDGE_TAG_KEYS
#         + usage_fields
#     )

#     with open(EDGES_FILE, "w", newline="") as f:
#         writer = csv.DictWriter(f, fieldnames=edge_fieldnames)
#         writer.writeheader()
#         for e in edges:
#             writer.writerow(e)

#     print(f"✅ size={size} 完成")
#     print(f"   nodes: {NODES_FILE}")
#     print(f"   edges: {EDGES_FILE}")

import csv
import os

# ======================
# 规模列表：70,80,...,120
# ======================
SIZES = list(range(10, 121, 10))

INPUT_DIR = "/home/luwenqi/influxdb-final-test-dir/data/cpu-only"
OUTPUT_DIR = "/home/luwenqi/sdata"

# ======================
# 节点静态属性（不随时间变化）
# ======================
SERVICE_NODE_KEYS = [
    "service_environment",
    "os",
    "arch",
]

HOST_NODE_KEYS = [
    "team",
    "service_version",
]

# ======================
# 边标签属性（结构属性）
# ======================
EDGE_TAG_KEYS = [
    "region",
    "datacenter",
    "rack",
    "service",
]

# ======================
# 解析 Influx 行
# ======================
def parse_line(line: str):
    meta, rest = line.strip().split(" ", 1)
    fields_part, ts = rest.rsplit(" ", 1)

    _, tags_part = meta.split(",", 1)
    tag_kv = dict(kv.split("=", 1) for kv in tags_part.split(","))

    field_kv = {}
    for kv in fields_part.split(","):
        k, v = kv.split("=")
        field_kv[k] = int(v.rstrip("i"))

    return tag_kv, field_kv, int(ts)

# ======================
# 主流程
# ======================
for size in SIZES:
    print(f"\n🚀 Processing size = {size}")

    INPUT_FILE = os.path.join(INPUT_DIR, f"influx-cpu-only-{size}")
    NODES_FILE = os.path.join(OUTPUT_DIR, f"influx-cpu-only-{size}-nodes.csv")
    EDGES_FILE = os.path.join(OUTPUT_DIR, f"influx-cpu-only-{size}-edges.csv")

    nodes = {}  # id -> node dict

    # ======================
    # 打开 edges.csv（流式写）
    # ======================
    with open(EDGES_FILE, "w", newline="") as ef:
        edge_writer = None
        edge_fieldnames = None

        with open(INPUT_FILE, "r") as f:
            for line in f:
                if not line.strip():
                    continue

                tag_kv, field_kv, ts = parse_line(line)

                raw_src = tag_kv["source"]
                raw_dst = tag_kv["destination"]

                # ======================
                # Service Node（源节点）
                # ======================
                src_id = f"src_{raw_src}"
                if src_id not in nodes:
                    node = {"id": src_id}
                    for k in SERVICE_NODE_KEYS:
                        node[k] = tag_kv[k]
                    for k in HOST_NODE_KEYS:
                        node[k] = ""
                    nodes[src_id] = node

                # ======================
                # Host Node（目的节点）
                # ======================
                dst_id = f"dst_{raw_dst}"
                if dst_id not in nodes:
                    node = {"id": dst_id}
                    for k in HOST_NODE_KEYS:
                        node[k] = tag_kv[k]
                    for k in SERVICE_NODE_KEYS:
                        node[k] = ""
                    nodes[dst_id] = node

                # ======================
                # Edge Snapshot（直接写盘）
                # ======================
                edge = {
                    "source": src_id,
                    "destination": dst_id,
                    "timestamp": ts,
                }

                for k in EDGE_TAG_KEYS:
                    edge[k] = tag_kv[k]

                edge.update(field_kv)

                # 第一次写 edge，初始化 writer
                if edge_writer is None:
                    usage_fields = sorted(
                        k for k in field_kv.keys() if k.startswith("usage_")
                    )
                    edge_fieldnames = (
                        ["source", "destination", "timestamp"]
                        + EDGE_TAG_KEYS
                        + usage_fields
                    )
                    edge_writer = csv.DictWriter(
                        ef, fieldnames=edge_fieldnames
                    )
                    edge_writer.writeheader()

                edge_writer.writerow(edge)

    # ======================
    # 写 nodes.csv
    # ======================
    node_fieldnames = (
        ["id"]
        + SERVICE_NODE_KEYS
        + HOST_NODE_KEYS
    )

    with open(NODES_FILE, "w", newline="") as nf:
        writer = csv.DictWriter(nf, fieldnames=node_fieldnames)
        writer.writeheader()
        for node in nodes.values():
            writer.writerow(node)

    print(f"✅ size={size} 完成")
    print(f"   nodes: {NODES_FILE}")
    print(f"   edges: {EDGES_FILE}")

