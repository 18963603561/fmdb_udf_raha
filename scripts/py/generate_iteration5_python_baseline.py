"""使用 Raha Python demo 为迭代 5 固定数据生成可复现基线。"""

import hashlib
import gc
import glob
import json
import os
import random
import subprocess
import sys
import tempfile
import time

import numpy


SEED = 20260714
LABELING_BUDGET = 5


def coordinate(dataset, cell):
    """把 Python 行列下标转换成稳定的行标识和字段名称。"""
    row_index, column_index = cell
    return {
        "row_index": int(row_index),
        "row_id": str(dataset.dataframe.iloc[row_index]["id"]),
        "column_index": int(column_index),
        "column_name": str(dataset.dataframe.columns[column_index]),
    }


def file_md5(path):
    """计算输入文件哈希，防止基线与测试数据静默错配。"""
    digest = hashlib.md5()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(8192), b""):
            digest.update(block)
    return digest.hexdigest()


def demo_revision(demo_root):
    """读取 Python demo 当前提交，非 Git 环境返回明确占位。"""
    try:
        return subprocess.check_output(
            ["git", "-C", demo_root, "rev-parse", "HEAD"],
            text=True,
            encoding="utf-8",
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return "UNKNOWN"


def selected_od_baseline(application, dataset):
    """运行代表性 dBoost 配置，并兼容 Windows 临时文件延迟释放。"""
    configurations = [
        ["gaussian", "3.0"],
        ["histogram", "0.1", "0.1"],
    ]
    profiles = []
    real_remove = os.remove

    def tolerant_remove(path):
        try:
            real_remove(path)
        except PermissionError:
            # dBoost 在 Windows 下可能延迟释放输入文件，流程结束后统一清理。
            pass

    os.remove = tolerant_remove
    try:
        for configuration in configurations:
            profile = application._strategy_runner_process(
                (dataset, "OD", configuration)
            )
            profiles.append({
                "family": "OD",
                "configuration": configuration,
                "cells": [coordinate(dataset, cell)
                          for cell in sorted(profile["output"])],
            })
    finally:
        os.remove = real_remove
        gc.collect()
        time.sleep(0.2)
        pattern = os.path.join(tempfile.gettempdir(), dataset.name + "-*.csv*")
        for path in glob.glob(pattern):
            real_remove(path)
    return profiles


def run(demo_root, dirty_path, clean_path):
    """执行 Python demo 的 PVD、RVD、特征、聚类、采样和检测流程。"""
    sys.path.insert(0, demo_root)
    from raha.detection import Detection

    random.seed(SEED)
    numpy.random.seed(SEED)
    application = Detection()
    application.LABELING_BUDGET = LABELING_BUDGET
    application.SAVE_RESULTS = False
    application.VERBOSE = False
    application.ERROR_DETECTION_ALGORITHMS = ["PVD", "RVD"]
    application.CLASSIFICATION_MODEL = "GBC"
    application.LABEL_PROPAGATION_METHOD = "homogeneity"
    dataset = application.initialize_dataset({
        "name": "iteration5-alignment",
        "path": dirty_path,
        "clean_path": clean_path,
    })

    od_profiles = selected_od_baseline(application, dataset)
    application.run_strategies(dataset)
    application.generate_features(dataset)
    application.build_clusters(dataset)
    sampled_rows = []
    while len(dataset.labeled_tuples) < LABELING_BUDGET:
        application.sample_tuple(dataset)
        sampled_rows.append(int(dataset.sampled_tuple))
        application.label_with_ground_truth(dataset)
    application.propagate_labels(dataset)
    application.predict_labels(dataset)

    strategies = []
    for profile in sorted(dataset.strategy_profiles, key=lambda item: item["name"]):
        strategy_name, configuration = json.loads(profile["name"])
        strategies.append({
            "family": strategy_name,
            "configuration": configuration,
            "cells": [coordinate(dataset, cell)
                      for cell in sorted(profile["output"])],
        })
    feature_columns = []
    for column_index, matrix in enumerate(dataset.column_features):
        feature_columns.append({
            "column_index": column_index,
            "column_name": str(dataset.dataframe.columns[column_index]),
            "row_count": int(matrix.shape[0]),
            "feature_count": int(matrix.shape[1]),
            "nonzero_count": int(numpy.count_nonzero(matrix)),
        })
    cluster_counts = {}
    for cluster_count in range(2, LABELING_BUDGET + 2):
        cluster_counts[str(cluster_count)] = {
            str(dataset.dataframe.columns[column_index]): len(clusters)
            for column_index, clusters
            in dataset.clusters_k_j_c_ce[cluster_count].items()
        }
    actual_errors = [coordinate(dataset, cell)
                     for cell in sorted(dataset.get_actual_errors_dictionary())]
    detected_cells = [coordinate(dataset, cell)
                      for cell in sorted(dataset.detected_cells)]
    return {
        "metadata": {
            "python_demo_root": os.path.abspath(demo_root),
            "python_demo_revision": demo_revision(demo_root),
            "detection_source": os.path.join(os.path.abspath(demo_root), "raha", "detection.py"),
            "algorithms": application.ERROR_DETECTION_ALGORITHMS,
            "selected_od_configurations": [
                profile["configuration"] for profile in od_profiles
            ],
            "random_seed": SEED,
            "labeling_budget": LABELING_BUDGET,
            "dirty_md5": file_md5(dirty_path),
            "clean_md5": file_md5(clean_path),
        },
        "strategy": {
            "profile_count": len(strategies),
            "profiles": strategies,
            "selected_od_profiles": od_profiles,
        },
        "feature": {
            "columns": feature_columns,
        },
        "sampling": {
            "sampled_row_indexes": sampled_rows,
            "cluster_counts": cluster_counts,
        },
        "detection": {
            "actual_errors": actual_errors,
            "detected_cells": detected_cells,
        },
    }


def write_properties(path, baseline):
    """写出 Java 无第三方解析依赖即可读取的摘要属性。"""
    detected = baseline["detection"]["detected_cells"]
    actual = baseline["detection"]["actual_errors"]
    feature_counts = baseline["feature"]["columns"]
    selected_profiles = {}
    for profile in baseline["strategy"]["profiles"]:
        key = profile["family"] + "|" + json.dumps(
            profile["configuration"], ensure_ascii=False, separators=(",", ":")
        )
        selected_profiles[key] = profile["cells"]
    for profile in baseline["strategy"]["selected_od_profiles"]:
        key = profile["family"] + "|" + json.dumps(
            profile["configuration"], ensure_ascii=False, separators=(",", ":")
        )
        selected_profiles[key] = profile["cells"]

    def cells(profile_key):
        return ",".join(
            item["row_id"] + ":" + item["column_name"]
            for item in selected_profiles.get(profile_key, [])
        )

    values = {
        "python.demo.revision": baseline["metadata"]["python_demo_revision"],
        "dirty.md5": baseline["metadata"]["dirty_md5"],
        "clean.md5": baseline["metadata"]["clean_md5"],
        "strategy.profile.count": str(baseline["strategy"]["profile_count"]),
        "feature.counts": ",".join(
            item["column_name"] + ":" + str(item["feature_count"])
            for item in feature_counts
        ),
        "sampled.row.indexes": ",".join(
            str(index) for index in baseline["sampling"]["sampled_row_indexes"]
        ),
        "rvd.code.city.cells": cells('RVD|["code","city"]'),
        "pvd.event_date.slash.cells": cells('PVD|["event_date","/"]'),
        "od.gaussian.3.cells": cells('OD|["gaussian","3.0"]'),
        "od.histogram.0.1.0.1.count": str(len(
            selected_profiles.get('OD|["histogram","0.1","0.1"]', [])
        )),
        "actual.error.cells": ",".join(
            item["row_id"] + ":" + item["column_name"] for item in actual
        ),
        "detected.cells": ",".join(
            item["row_id"] + ":" + item["column_name"] for item in detected
        ),
    }
    with open(path, "w", encoding="utf-8", newline="\n") as stream:
        stream.write("# Raha Python demo 迭代 5 基线摘要\n")
        for key in sorted(values):
            stream.write(key + "=" + values[key] + "\n")


def main():
    """解析固定路径并生成详细 JSON 和摘要属性。"""
    workspace = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    demo_root = os.environ.get("RAHA_PYTHON_DEMO_ROOT", r"F:\ai-code\raha\raha-master")
    resource_root = os.path.join(workspace, "src", "test", "resources", "alignment")
    dirty_path = os.path.join(resource_root, "iteration5-dirty.csv")
    clean_path = os.path.join(resource_root, "iteration5-clean.csv")
    baseline = run(demo_root, dirty_path, clean_path)
    json_path = os.path.join(resource_root, "iteration5-python-baseline.json")
    properties_path = os.path.join(resource_root, "iteration5-python-baseline.properties")
    with open(json_path, "w", encoding="utf-8", newline="\n") as stream:
        json.dump(baseline, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
    write_properties(properties_path, baseline)
    print(json_path)
    print(properties_path)


if __name__ == "__main__":
    main()
