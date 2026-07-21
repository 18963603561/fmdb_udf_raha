#!/usr/bin/env python3
"""将 Python Raha 策略缓存导出为稳定的逐策略 JSONL 对齐产物。"""

import argparse
import csv
import hashlib
import json
import pickle
from pathlib import Path


def parse_args():
    """解析数据文件、策略缓存目录和输出路径。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dirty", required=True)
    parser.add_argument("--strategy-dir", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def md5_lines(values):
    """对排序后的坐标文本生成稳定摘要。"""
    return hashlib.md5("\n".join(values).encode("utf-8")).hexdigest()


def load_table(path):
    """读取脏数据并返回字段名和零基行号到稳定行标识的映射。"""
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.reader(stream))
    if not rows:
        raise ValueError("脏数据不能为空")
    columns = rows[0]
    row_ids = [row[0] for row in rows[1:]]
    return columns, row_ids


def canonical_key(name):
    """将 Python 策略名称转换为跨语言可映射的规范键。"""
    algorithm, configuration = json.loads(name)
    if algorithm == "RVD":
        return "RVD|{}|{}".format(configuration[0], configuration[1])
    if algorithm == "PVD":
        return "PVD|{}|{}".format(configuration[0], configuration[1])
    return "OD|" + "|".join(str(value) for value in configuration)


def main():
    """读取全部缓存并按规范配置顺序写出逐策略坐标。"""
    args = parse_args()
    dirty_path = Path(args.dirty)
    strategy_dir = Path(args.strategy_dir)
    output_path = Path(args.output)
    columns, row_ids = load_table(dirty_path)
    artifacts = []
    for cache_path in sorted(strategy_dir.iterdir(), key=lambda item: item.name):
        if not cache_path.is_file():
            continue
        # Python Raha 现有缓存使用 pickle，导出过程只读取用户明确指定的本地目录。
        with cache_path.open("rb") as stream:
            profile = pickle.load(stream)
        name = profile["name"]
        algorithm, _ = json.loads(name)
        coordinates = []
        for row_index, column_index in profile.get("output", []):
            if row_index < 0 or row_index >= len(row_ids):
                raise ValueError("策略缓存行号超出脏数据范围")
            if column_index < 0 or column_index >= len(columns):
                raise ValueError("策略缓存列号超出脏数据范围")
            coordinates.append("{}|{}".format(
                row_ids[row_index], columns[column_index]))
        coordinates.sort()
        artifacts.append({
            "source": "PYTHON",
            "family": algorithm,
            "strategyId": hashlib.md5(name.encode("utf-8")).hexdigest(),
            "strategyType": algorithm,
            "canonicalConfiguration": canonical_key(name),
            "configurationHash": hashlib.md5(name.encode("utf-8")).hexdigest(),
            "status": "SUCCEEDED",
            "candidateCount": len(coordinates),
            "coordinateHash": md5_lines(coordinates),
            "runtimeMillis": int(round(float(profile.get("runtime", 0.0)) * 1000.0)),
            "coordinates": coordinates,
            "pythonName": name,
        })
    artifacts.sort(key=lambda item: item["canonicalConfiguration"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as stream:
        for artifact in artifacts:
            stream.write(json.dumps(artifact, ensure_ascii=False,
                                    sort_keys=True, separators=(",", ":")))
            stream.write("\n")
    print(json.dumps({"strategyCount": len(artifacts),
                      "output": str(output_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
