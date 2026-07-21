"""使用固定配置运行任意 Python Raha 数据集并输出评测摘要。"""

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


LOGGER = logging.getLogger("python-dataset-comparison")
ORIGINAL_REMOVE = os.remove


def remove_with_windows_retry(path):
    """兼容 Windows 上外部检测进程短暂占用临时文件的情况。"""
    last_error = None
    for attempt in range(3):
        try:
            return ORIGINAL_REMOVE(path)
        except PermissionError as exception:
            last_error = exception
            time.sleep(0.05 * (attempt + 1))
    LOGGER.warning("临时文件仍被占用，延迟到父进程退出后清理，path=%s，error=%s",
                   path, last_error)
    return None


if os.name == "nt":
    os.remove = remove_with_windows_retry


def parse_args():
    """解析 Python 工程、数据集、输出路径和随机种子。"""
    parser = argparse.ArgumentParser(description="运行 Python Raha 数据集基线")
    parser.add_argument("--raha-root", required=True)
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--dirty", required=True)
    parser.add_argument("--clean", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=20260715)
    return parser.parse_args()


def cells_by_column(cells, columns):
    """按字段汇总单元格坐标数量。"""
    return {
        columns[column_index]: sum(
            1 for _, current_column in cells
            if current_column == column_index)
        for column_index in range(len(columns))
    }


def main():
    """执行 Python Raha 并写出可复核的结构化摘要。"""
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raha_root = os.path.abspath(args.raha_root)
    sys.path.insert(0, raha_root)
    from raha.dataset import Dataset
    from raha.detection import Detection

    random.seed(args.seed)
    numpy.random.seed(args.seed)
    dataset_dictionary = {
        "name": args.dataset_name,
        "path": os.path.abspath(args.dirty),
        "clean_path": os.path.abspath(args.clean),
    }
    app = Detection()
    app.LABELING_BUDGET = 20
    app.USER_LABELING_ACCURACY = 1.0
    app.VERBOSE = False
    app.SAVE_RESULTS = True
    app.CLUSTERING_BASED_SAMPLING = True
    app.STRATEGY_FILTERING = False
    app.CLASSIFICATION_MODEL = "GBC"
    app.LABEL_PROPAGATION_METHOD = "homogeneity"
    app.ERROR_DETECTION_ALGORITHMS = ["OD", "PVD", "RVD"]

    LOGGER.info("开始执行 Python Raha，dataset=%s，seed=%s，dirty=%s",
                args.dataset_name, args.seed, dataset_dictionary["path"])
    started_at = time.time()
    try:
        detected = app.run(dataset_dictionary)
    except Exception:
        LOGGER.exception("Python Raha 数据集执行失败，dataset=%s",
                         args.dataset_name)
        raise
    elapsed_seconds = time.time() - started_at

    dataset = Dataset(dataset_dictionary)
    actual = set(dataset.get_actual_errors_dictionary())
    detected_cells = set(detected)
    true_positive = detected_cells & actual
    false_positive = detected_cells - actual
    false_negative = actual - detected_cells
    precision, recall, f1 = dataset.get_data_cleaning_evaluation(detected)[:3]
    detection_path = os.path.join(
        os.path.dirname(dataset_dictionary["path"]),
        "raha-baran-results-" + args.dataset_name,
        "error-detection", "detection.dataset")
    LOGGER.info("读取 Python Raha 检测产物，path=%s", detection_path)
    with open(detection_path, "rb") as stream:
        persisted = pickle.load(stream)
    columns = dataset.dataframe.columns.tolist()
    result = {
        "datasetName": args.dataset_name,
        "dirtyPath": dataset_dictionary["path"],
        "cleanPath": dataset_dictionary["clean_path"],
        "detectionPath": detection_path,
        "seed": args.seed,
        "pythonVersion": sys.version,
        "numpyVersion": numpy.__version__,
        "pandasVersion": pandas.__version__,
        "scipyVersion": scipy.__version__,
        "sklearnVersion": sklearn.__version__,
        "rowCount": int(dataset.dataframe.shape[0]),
        "columnCount": int(dataset.dataframe.shape[1]),
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
        "actualErrorCount": len(actual),
        "detectedCount": len(detected_cells),
        "truePositive": len(true_positive),
        "falsePositive": len(false_positive),
        "falseNegative": len(false_negative),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "actualByColumn": cells_by_column(actual, columns),
        "detectedByColumn": cells_by_column(detected_cells, columns),
        "truePositiveByColumn": cells_by_column(true_positive, columns),
        "falsePositiveByColumn": cells_by_column(false_positive, columns),
        "falseNegativeByColumn": cells_by_column(false_negative, columns),
    }
    output_path = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="\n") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    LOGGER.info("Python Raha 执行完成，dataset=%s，precision=%.6f，"
                "recall=%.6f，f1=%.6f，elapsedSeconds=%.3f",
                args.dataset_name, precision, recall, f1, elapsed_seconds)


if __name__ == "__main__":
    main()
