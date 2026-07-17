#!/usr/bin/env python3
"""比较 Python 与 Java 逐策略产物并输出严格映射和差异摘要。"""

import argparse
import json
from pathlib import Path


def parse_args():
    """解析两侧 JSONL 和比较结果输出路径。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--python", required=True)
    parser.add_argument("--java", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--row-id-column", default="tuple_id")
    return parser.parse_args()


def load_jsonl(path):
    """读取 JSONL 策略列表。"""
    with path.open("r", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def java_key(item):
    """将 Java RVD 配置转换为 Python RVD 规范键。"""
    if item["family"] != "RVD":
        return None
    parts = item["canonicalConfiguration"].split("|")
    if len(parts) < 3:
        return None
    return "RVD|{}|{}".format(parts[1], parts[2])


def coordinate_metrics(python_item, java_item):
    """计算一对策略的坐标交并差指标。"""
    python_cells = set(python_item["coordinates"])
    java_cells = set(java_item["coordinates"])
    intersection = python_cells & java_cells
    union = python_cells | java_cells
    precision = len(intersection) / len(java_cells) if java_cells else 0.0
    recall = len(intersection) / len(python_cells) if python_cells else 0.0
    return {
        "intersection": len(intersection),
        "pythonOnly": len(python_cells - java_cells),
        "javaOnly": len(java_cells - python_cells),
        "precision": precision,
        "recall": recall,
        "jaccard": len(intersection) / len(union) if union else 1.0,
        "exact": python_cells == java_cells,
    }


def main():
    """严格比较可映射 RVD，并汇总其他策略族的语义状态。"""
    args = parse_args()
    python_items = load_jsonl(Path(args.python))
    java_items = load_jsonl(Path(args.java))
    python_rvd = {item["canonicalConfiguration"]: item
                  for item in python_items if item["family"] == "RVD"}
    java_rvd = {java_key(item): item for item in java_items
                if java_key(item) is not None}
    mappings = []
    for key in sorted(python_rvd):
        java_item = java_rvd.get(key)
        if java_item is None:
            parts = key.split("|")
            # 稳定行标识不参与 Java 检测，相关有向字段对属于明确的不适用范围。
            not_applicable = (len(parts) >= 3
                              and args.row_id_column in parts[1:3])
            mappings.append({"key": key, "status": "NOT_APPLICABLE"
                             if not_applicable else "MISSING"})
            continue
        metrics = coordinate_metrics(python_rvd[key], java_item)
        mappings.append({"key": key,
                         "status": "EXACT" if metrics["exact"] else "APPROXIMATE",
                         "metrics": metrics})
    exact_count = sum(1 for item in mappings if item["status"] == "EXACT")
    approximate_count = sum(1 for item in mappings
                            if item["status"] == "APPROXIMATE")
    result = {
        "pythonStrategyCount": len(python_items),
        "javaStrategyCount": len(java_items),
        "pythonFamilyCounts": family_counts(python_items),
        "javaFamilyCounts": family_counts(java_items),
        "rvdMappings": mappings,
        "rvdExactCount": exact_count,
        "rvdApproximateCount": approximate_count,
        "pvdStatus": "MISSING_PYTHON_OBSERVED_CHARACTER_GRID",
        "odStatus": "APPROXIMATE_SPARK_NATIVE_IMPLEMENTATION",
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False,
                                      indent=2, sort_keys=True) + "\n",
                           encoding="utf-8", newline="\n")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


def family_counts(items):
    """统计策略族数量。"""
    counts = {}
    for item in items:
        counts[item["family"]] = counts.get(item["family"], 0) + 1
    return counts


if __name__ == "__main__":
    main()
