"""运行 Python Raha flights 对比基线并输出坐标级结果。"""

import argparse
import json
import logging
import os
import pickle
import random
import sys
import time

import numpy
import pandas
import scipy
import sklearn


LOGGER = logging.getLogger("flights-python-comparison")
ORIGINAL_REMOVE = os.remove


def remove_with_windows_retry(path):
    """兼容 Windows 上 dBoost 子进程短暂占用临时文件的情况。"""
    last_error = None
    for attempt in range(3):
        try:
            return ORIGINAL_REMOVE(path)
        except PermissionError as exception:
            # dBoost 返回后文件句柄可能短暂未释放，有限退避后再次删除。
            last_error = exception
            time.sleep(0.05 * (attempt + 1))
    # 策略输出已经读取完成，临时输入清理失败不应覆盖有效检测结果。
    LOGGER.warning("Windows 临时文件仍被占用，延迟到父进程结束后清理，path=%s，error=%s",
                   path, last_error)
    return None


# Windows 多进程会重新导入主模块，因此兼容层必须在模块加载阶段安装。
if os.name == "nt":
    os.remove = remove_with_windows_retry


def parse_args():
    """解析 Python Raha 源码、输入文件、输出文件和随机种子。"""
    parser = argparse.ArgumentParser(description="运行 Python Raha flights 对比基线")
    parser.add_argument("--raha-root", required=True)
    parser.add_argument("--dirty", required=True)
    parser.add_argument("--clean", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--seeds", default="20260715,20260716,20260717")
    return parser.parse_args()


def cells_by_column(cells, columns):
    """按字段汇总单元格坐标数量。"""
    return {
        columns[column_index]: sum(1 for _, current_column in cells
                                   if current_column == column_index)
        for column_index in range(len(columns))
    }


def run_once(detection_type, dataset_type, dataset_dictionary, seed):
    """使用固定随机种子执行一次完整 Python Raha 检测。"""
    random.seed(seed)
    numpy.random.seed(seed)
    app = detection_type()
    app.LABELING_BUDGET = 20
    app.USER_LABELING_ACCURACY = 1.0
    app.VERBOSE = False
    app.SAVE_RESULTS = True
    app.CLUSTERING_BASED_SAMPLING = True
    app.STRATEGY_FILTERING = False
    app.CLASSIFICATION_MODEL = "GBC"
    app.LABEL_PROPAGATION_METHOD = "homogeneity"
    app.ERROR_DETECTION_ALGORITHMS = ["OD", "PVD", "RVD"]

    LOGGER.info("开始执行 Python Raha，seed=%s，dirty=%s", seed,
                dataset_dictionary["path"])
    started_at = time.time()
    detected = app.run(dataset_dictionary)
    elapsed_seconds = time.time() - started_at
    dataset = dataset_type(dataset_dictionary)
    actual = dataset.get_actual_errors_dictionary()
    detected_cells = set(detected)
    actual_cells = set(actual)
    true_positive = detected_cells & actual_cells
    false_positive = detected_cells - actual_cells
    false_negative = actual_cells - detected_cells
    precision, recall, f1 = dataset.get_data_cleaning_evaluation(detected)[:3]

    result_path = os.path.join(
        os.path.dirname(dataset_dictionary["path"]),
        "raha-baran-results-" + dataset_dictionary["name"],
        "error-detection", "detection.dataset")
    LOGGER.info("开始读取 Python Raha 检测产物，seed=%s，path=%s", seed, result_path)
    with open(result_path, "rb") as stream:
        persisted = pickle.load(stream)
    columns = dataset.dataframe.columns.tolist()
    result = {
        "seed": seed,
        "elapsedSeconds": elapsed_seconds,
        "labelingBudget": app.LABELING_BUDGET,
        "labeledTupleCount": len(persisted.labeled_tuples),
        "labeledTuples": sorted(int(value) for value in persisted.labeled_tuples),
        "propagatedLabelCount": len(persisted.extended_labeled_cells),
        "strategyCount": len(persisted.strategy_profiles),
        "featureCountByColumn": {
            columns[index]: int(persisted.column_features[index].shape[1])
            for index in range(len(columns))
        },
        "actualErrorCount": len(actual_cells),
        "detectedCount": len(detected_cells),
        "truePositive": len(true_positive),
        "falsePositive": len(false_positive),
        "falseNegative": len(false_negative),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "actualByColumn": cells_by_column(actual_cells, columns),
        "detectedByColumn": cells_by_column(detected_cells, columns),
        "truePositiveByColumn": cells_by_column(true_positive, columns),
        "falsePositiveByColumn": cells_by_column(false_positive, columns),
        "falseNegativeByColumn": cells_by_column(false_negative, columns),
        "detectedCells": [[int(row), int(column)]
                          for row, column in sorted(detected_cells)],
    }
    LOGGER.info("Python Raha 执行完成，seed=%s，detected=%s，precision=%.6f，"
                "recall=%.6f，f1=%.6f，elapsedSeconds=%.3f",
                seed, len(detected_cells), precision, recall, f1, elapsed_seconds)
    return result


def main():
    """执行多随机种子基线并写出统一 JSON。"""
    args = parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raha_root = os.path.abspath(args.raha_root)
    sys.path.insert(0, raha_root)
    from raha.dataset import Dataset
    from raha.detection import Detection

    dataset_dictionary = {
        "name": "flights_compare",
        "path": os.path.abspath(args.dirty),
        "clean_path": os.path.abspath(args.clean),
    }
    seeds = [int(value.strip()) for value in args.seeds.split(",") if value.strip()]
    try:
        runs = [run_once(Detection, Dataset, dataset_dictionary, seed)
                for seed in seeds]
        output = {
            "pythonVersion": sys.version,
            "numpyVersion": numpy.__version__,
            "pandasVersion": pandas.__version__,
            "scipyVersion": scipy.__version__,
            "sklearnVersion": sklearn.__version__,
            "dirtyPath": dataset_dictionary["path"],
            "cleanPath": dataset_dictionary["clean_path"],
            "runs": runs,
        }
        output_path = os.path.abspath(args.output)
        LOGGER.info("开始写入 Python Raha 对比结果，path=%s", output_path)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(output, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        LOGGER.info("Python Raha 对比结果写入完成，path=%s", output_path)
    except Exception:
        LOGGER.exception("Python Raha flights 对比执行失败")
        raise


if __name__ == "__main__":
    main()
