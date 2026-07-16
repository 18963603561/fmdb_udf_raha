"""按单元格坐标比较 Python Raha 与 Java Spark 检测结果。"""

import argparse
import json
import logging
import os
import pickle
import statistics
import sys


LOGGER = logging.getLogger("flights-result-comparison")


def parse_args():
    """解析 Python 工程、Java 结果和输出路径。"""
    parser = argparse.ArgumentParser(description="比较 flights 两套检测结果")
    parser.add_argument("--raha-root", required=True)
    parser.add_argument("--java-results", required=True)
    parser.add_argument("--java-summary", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def metric_summary(detected, actual):
    """计算检测集合相对真值集合的混淆矩阵和三项指标。"""
    true_positive = detected & actual
    false_positive = detected - actual
    false_negative = actual - detected
    precision = 0.0 if not detected else len(true_positive) / len(detected)
    recall = 0.0 if not actual else len(true_positive) / len(actual)
    f1 = 0.0 if precision + recall == 0.0 else (
        2.0 * precision * recall / (precision + recall))
    return {
        "detectedCount": len(detected),
        "truePositive": len(true_positive),
        "falsePositive": len(false_positive),
        "falseNegative": len(false_negative),
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def parse_selected_thresholds(value):
    """解析 Java 验收摘要中的字段阈值映射。"""
    if not value:
        return {}
    text = value.strip()
    if text.startswith("{") and text.endswith("}"):
        text = text[1:-1]
    thresholds = {}
    for item in text.split(","):
        name, separator, threshold = item.strip().partition("=")
        if separator and name:
            thresholds[name] = float(threshold)
    return thresholds


def score_summary(values, selected_threshold):
    """汇总 Java 字段预测分数的范围、均值和超过实际阈值的数量。"""
    if not values:
        return None
    return {
        "count": len(values),
        "minimum": min(values),
        "maximum": max(values),
        "mean": statistics.fmean(values),
        "selectedThreshold": selected_threshold,
        "atOrAboveSelectedThreshold": sum(
            1 for value in values if value >= selected_threshold),
    }


def main():
    """加载两套正式产物并输出坐标级汇总。"""
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raha_root = os.path.abspath(args.raha_root)
    sys.path.insert(0, raha_root)
    from raha.dataset import Dataset

    flights_root = os.path.join(raha_root, "datasets", "flights")
    dataset_dictionary = {
        "name": "flights",
        "path": os.path.join(flights_root, "dirty.csv"),
        "clean_path": os.path.join(flights_root, "clean.csv"),
    }
    detection_path = os.path.join(
        flights_root,
        "raha-baran-results-flights",
        "error-detection",
        "detection.dataset")
    LOGGER.info("开始读取 Python Raha 正式检测产物，path=%s", detection_path)
    with open(detection_path, "rb") as stream:
        python_dataset = pickle.load(stream)
    dataset = Dataset(dataset_dictionary)
    columns = dataset.dataframe.columns.tolist()
    row_lookup = {
        str(value): int(index)
        for index, value in dataset.dataframe["tuple_id"].items()
    }
    actual = set(dataset.get_actual_errors_dictionary())
    python_detected = set(python_dataset.detected_cells)

    java_detected = set()
    java_scores = {column: [] for column in columns}
    java_result_count = 0
    LOGGER.info("开始读取 Java Spark 检测结果，path=%s", args.java_results)
    with open(args.java_results, "r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            record = json.loads(line)
            java_result_count += 1
            row_index = row_lookup[str(record["row_id"])]
            column_index = columns.index(record["column_name"])
            java_scores[record["column_name"]].append(float(record["score"]))
            if record["is_error"]:
                java_detected.add((row_index, column_index))
    with open(args.java_summary, "r", encoding="utf-8") as stream:
        java_summary = json.load(stream)
    selected_thresholds = parse_selected_thresholds(
        java_summary.get("selectedThresholds"))

    both = python_detected & java_detected
    python_only = python_detected - java_detected
    java_only = java_detected - python_detected
    by_column = {}
    for column_index, column_name in enumerate(columns):
        actual_column = {cell for cell in actual if cell[1] == column_index}
        python_column = {
            cell for cell in python_detected if cell[1] == column_index
        }
        java_column = {cell for cell in java_detected if cell[1] == column_index}
        by_column[column_name] = {
            "actualErrorCount": len(actual_column),
            "python": metric_summary(python_column, actual_column),
            "java": metric_summary(java_column, actual_column),
            "overlapDetectedCount": len(python_column & java_column),
            "pythonOnlyDetectedCount": len(python_column - java_column),
            "javaOnlyDetectedCount": len(java_column - python_column),
            "javaScore": score_summary(
                java_scores[column_name],
                selected_thresholds.get(column_name, 0.5)),
        }

    result = {
        "dataset": {
            "rowCount": int(dataset.dataframe.shape[0]),
            "columnCount": int(dataset.dataframe.shape[1]),
            "detectableColumnCount": int(dataset.dataframe.shape[1] - 1),
            "actualErrorCount": len(actual),
        },
        "python": {
            **metric_summary(python_detected, actual),
            "strategyCount": len(python_dataset.strategy_profiles),
            "labelingBudget": 20,
            "labeledTupleCount": len(python_dataset.labeled_tuples),
            "propagatedLabelCount": len(python_dataset.extended_labeled_cells),
        },
        "java": {
            **metric_summary(java_detected, actual),
            "resultRowCount": java_result_count,
            "candidateModelCount": java_summary["candidateModelCount"],
            "elapsedMillis": java_summary["elapsedMillis"],
        },
        "overlap": {
            "bothDetectedCount": len(both),
            "bothTruePositive": len(both & actual),
            "bothFalsePositive": len(both - actual),
            "pythonOnlyDetectedCount": len(python_only),
            "pythonOnlyTruePositive": len(python_only & actual),
            "pythonOnlyFalsePositive": len(python_only - actual),
            "javaOnlyDetectedCount": len(java_only),
            "javaOnlyTruePositive": len(java_only & actual),
            "javaOnlyFalsePositive": len(java_only - actual),
            "detectedJaccard": (
                0.0
                if not (python_detected | java_detected)
                else len(both) / len(python_detected | java_detected)),
        },
        "byColumn": by_column,
    }
    output_path = os.path.abspath(args.output)
    LOGGER.info("开始写入检测结果对比摘要，path=%s", output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="\n") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    LOGGER.info("检测结果对比摘要写入完成，path=%s", output_path)


if __name__ == "__main__":
    main()
