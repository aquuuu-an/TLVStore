import random
import os

# ======================
# 规模列表：10,20,...,120
# ======================
SIZES = list(range(10, 121, 10))

INPUT_DIR = "/home/luwenqi/cpu-only-origin"
OUTPUT_DIR = "/home/luwenqi/influxdb-final-test-dir/data/cpu-only"

EXPAND_FACTOR = 9

MIN_STEP_SEC = 30
MAX_STEP_SEC = 120

FIELDS = [
    "usage_user",
    "usage_system",
    "usage_idle",
    "usage_nice",
    "usage_iowait",
    "usage_irq",
    "usage_softirq",
    "usage_steal",
    "usage_guest",
    "usage_guest_nice",
]

# ======================
# 工具函数
# ======================
def random_fields():
    return ",".join(
        f"{f}={random.randint(0, 100)}i"
        for f in FIELDS
    )

# ======================
# 主逻辑：按规模循环
# ======================
for size in SIZES:
    print(f"\n🚀 Processing size = {size}")

    INPUT_FILE = os.path.join(
        INPUT_DIR, f"influx-cpu-only-{size}"
    )
    OUTPUT_FILE = os.path.join(
        OUTPUT_DIR, f"influx-cpu-only-{size}"
    )

    all_lines = []

    with open(INPUT_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # measurement+tags | fields | timestamp
            left, _, ts_part = line.rsplit(" ", 2)
            base_ts = int(ts_part)

            # 原始数据
            all_lines.append((base_ts, line))

            # 扩展数据（时间随机递增）
            current_ts = base_ts
            for _ in range(EXPAND_FACTOR):
                delta_sec = random.randint(
                    MIN_STEP_SEC, MAX_STEP_SEC
                )
                current_ts += delta_sec * 1_000_000_000

                new_fields = random_fields()
                new_line = f"{left} {new_fields} {current_ts}"
                all_lines.append((current_ts, new_line))

    # ======================
    # 全局按时间排序
    # ======================
    all_lines.sort(key=lambda x: x[0])

    # ======================
    # 写出
    # ======================
    with open(OUTPUT_FILE, "w") as f:
        for _, line in all_lines:
            f.write(line + "\n")

    print(
        f"✅ size={size} 完成："
        f"每条原始数据扩展 {EXPAND_FACTOR + 1} 倍，"
        f"时间递增范围 [{MIN_STEP_SEC}s, {MAX_STEP_SEC}s]"
    )
    print(f"   input : {INPUT_FILE}")
    print(f"   output: {OUTPUT_FILE}")
