# import re
# import os
# import sys


# def transform_single_query(query: str) -> str:
#     """
#     转换单条 MATCH (n:label) 查询
#     """

#     match_pattern = r"MATCH\s*\(\s*(\w+)\s*:\s*(\w+)\s*\)"

#     def replacer(match):
#         var_name = match.group(1)
#         label = match.group(2)

#         # 替换 (n:label) -> (n)
#         replaced = f"MATCH ({var_name})"

#         return replaced

#     # 1️⃣ 替换所有 MATCH (n:label)
#     new_query = re.sub(match_pattern, replacer, query, flags=re.IGNORECASE)

#     # 2️⃣ 处理 WHERE 插入
#     matches = list(re.finditer(match_pattern, query, flags=re.IGNORECASE))

#     for m in matches:
#         var_name = m.group(1)
#         label = m.group(2)

#         # 插入 WHERE 条件
#         new_query = re.sub(
#             r"\bWHERE\b\s+",
#             f"WHERE {var_name}.service_environment='{label}' AND ",
#             new_query,
#             count=1,
#             flags=re.IGNORECASE
#         )

#     return new_query


# def transform_file(content: str) -> str:
#     """
#     按行处理整个文件
#     """
#     lines = content.splitlines()
#     transformed_lines = []

#     for line in lines:
#         if "MATCH" in line and ":" in line:
#             line = transform_single_query(line)
#         transformed_lines.append(line)

#     return "\n".join(transformed_lines)


# def main():

#     input_file = "queries_aeong/point_v_vertexTag.cypher"

#     if not os.path.exists(input_file):
#         print("File not found:", input_file)
#         sys.exit(1)

#     with open(input_file, "r") as f:
#         content = f.read()

#     transformed = transform_file(content)

#     output_file = input_file.replace(".cypher", "_converted.cypher")

#     with open(output_file, "w") as f:
#         f.write(transformed)

#     print("Conversion complete.")
#     print("Output file:", output_file)


# if __name__ == "__main__":
#     main()
import re
import os
from datetime import datetime, timezone

# =========================
# ISO8601 → 纳秒时间戳
# =========================
def iso_to_ns(iso_str: str) -> int:
    dt = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


# =========================
# 替换 TT FROM TO 时间
# =========================
def convert_line(line: str) -> str:
    pattern = r"TT FROM '([^']+)' TO '([^']+)'"

    def replacer(match):
        start_iso = match.group(1)
        end_iso = match.group(2)

        start_ns = iso_to_ns(start_iso)
        end_ns = iso_to_ns(end_iso)

        return f"TT FROM {start_ns} TO {end_ns}"

    return re.sub(pattern, replacer, line)


# =========================
# 主函数
# =========================
def main(input_file, output_file):

    # ✅ 自动创建输出目录
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    new_lines = [convert_line(line) for line in lines]

    with open(output_file, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    print(f"转换完成，输出文件：{output_file}")


if __name__ == "__main__":
    input_file = "queries_aeong/path_reachable.cypher"
    output_file = "queries_aeong2/path_reachable.cypher"

    main(input_file, output_file)