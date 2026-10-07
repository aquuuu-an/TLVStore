import csv

NODES_FILE = "/home/luwenqi/data/influx-cpu-only-10-nodes.csv"

REQUIRED_FIELDS = ["id"]
OPTIONAL_FIELDS = [
    "service_environment",
    "os",
    "arch",
    "team",
    "service_version",
]

print(f"🔍 开始检查节点文件: {NODES_FILE}\n")

with open(NODES_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    print("📌 CSV 表头字段：")
    for i, h in enumerate(reader.fieldnames):
        print(f"  [{i}] '{h}'")
    print()

    if "id" not in reader.fieldnames:
        print("❌ 严重错误：CSV 中不存在 'id' 字段！")
        exit(1)

    bad_rows = 0
    total = 0

    for lineno, row in enumerate(reader, start=2):  # 表头占第1行
        total += 1

        raw_id = row.get("id")

        # ---------- 致命错误 ----------
        if raw_id is None:
            print(f"❌ 行 {lineno}: 缺失字段 'id' -> {row}")
            bad_rows += 1
            continue

        vid = raw_id.strip()
        if not vid:
            print(f"❌ 行 {lineno}: id 为空或空白 -> {row}")
            bad_rows += 1
            continue

        # ---------- 非致命但需要注意 ----------
        for k in OPTIONAL_FIELDS:
            if k not in row:
                print(f"⚠️ 行 {lineno}: 缺失字段 '{k}'")
            elif row[k] == "":
                print(f"⚠️ 行 {lineno}: 字段 '{k}' 为空")

        # ---------- 不可见字符 ----------
        if any(ord(c) < 32 for c in vid):
            print(f"⚠️ 行 {lineno}: id 含不可见字符: {repr(vid)}")

    print("\n====== 检查完成 ======")
    print(f"总行数: {total}")
    print(f"问题行数: {bad_rows}")

    if bad_rows == 0:
        print("✅ 没有发现会导致 Neo4j 插入失败的节点行")
    else:
        print("🚨 请先修复上述行，再执行插入脚本")
