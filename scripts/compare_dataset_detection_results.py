"""按单元格坐标比较任意 Python Raha 与 Java Spark 检测结果。"""

import argparse
import glob
import json
import logging
import os
import pickle
import statistics
import sys


LOGGER = logging.getLogger("dataset-result-comparison")


def parse_args():
    """解析数据集、双边结果和输出路径。"""
    parser = argparse.ArgumentParser(description="比较 Raha 数据集检测结果")
    parser.add_argument("--raha-root", required=True)
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--dirty", required=True)
    parser.add_argument("--clean", required=True)
    parser.add_argument("--python-detection", required=True)
    parser.add_argument("--java-results", required=True)
    parser.add_argument("--java-summary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--java-row-id-column")
    parser.add_argument("--java-row-id-offset", type=int, default=0)
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
    """汇总 Java 字段预测分数和实际阈值。"""
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


def java_result_files(path):
    """返回 Java 单文件或 Spark JSON 目录中的结果文件。"""
    absolute = os.path.abspath(path)
    if os.path.isdir(absolute):
        files = sorted(glob.glob(os.path.join(absolute, "part-*.json")))
    else:
        files = [absolute]
    if not files:
        raise FileNotFoundError("Java 检测结果文件不存在：" + absolute)
    return files


def main():
    """加载两套结果并写出总体、字段和坐标重叠摘要。"""
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raha_root = os.path.abspath(args.raha_root)
    sys.path.insert(0, raha_root)
    from raha.dataset import Dataset

    dataset_dictionary = {
        "name": args.dataset_name,
        "path": os.path.abspath(args.dirty),
        "clean_path": os.path.abspath(args.clean),
    }
    dataset = Dataset(dataset_dictionary)
    columns = dataset.dataframe.columns.tolist()
    actual = set(dataset.get_actual_errors_dictionary())
    row_lookup = None
    if args.java_row_id_column:
        if args.java_row_id_column not in columns:
            raise ValueError("Java 行标识字段不在 Python 数据中")
        row_lookup = {
            str(value): int(index)
            for index, value in dataset.dataframe[
                args.java_row_id_column].items()
        }
        if len(row_lookup) != dataset.dataframe.shape[0]:
            raise ValueError("Java 行标识字段包含空值或重复值")
    with open(os.path.abspath(args.python_detection), "rb") as stream:
        python_dataset = pickle.load(stream)
    python_detected = set(python_dataset.detected_cells)

    java_detected = set()
    java_scores = {column: [] for column in columns}
    java_result_count = 0
    for result_file in java_result_files(args.java_results):
        LOGGER.info("读取 Java Spark 检测结果，path=%s", result_file)
        with open(result_file, "r", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                record = json.loads(line)
                java_result_count += 1
                row_index = (row_lookup[str(record["row_id"])]
                             if row_lookup is not None
                             else int(record["row_id"]) - args.java_row_id_offset)
                if row_index < 0 or row_index >= dataset.dataframe.shape[0]:
                    raise ValueError("Java 行标识超出 Python 数据范围")
                column_name = record["column_name"]
                column_index = columns.index(column_name)
                java_scores[column_name].append(float(record["score"]))
                if record["is_error"]:
                    java_detected.add((row_index, column_index))
    with open(os.path.abspath(args.java_summary), "r", encoding="utf-8") as stream:
        java_summary = json.load(stream)
    thresholds = parse_selected_thresholds(java_summary.get("selectedThresholds"))

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
                java_scores[column_name], thresholds.get(column_name, 0.5)),
        }
    result = {
        "dataset": {
            "name": args.dataset_name,
            "rowCount": int(dataset.dataframe.shape[0]),
            "columnCount": int(dataset.dataframe.shape[1]),
            "actualErrorCount": len(actual),
        },
        "python": {
            **metric_summary(python_detected, actual),
            "strategyCount": len(python_dataset.strategy_profiles),
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
                0.0 if not (python_detected | java_detected)
                else len(both) / len(python_detected | java_detected)),
        },
        "byColumn": by_column,
    }
    output_path = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="\n") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    LOGGER.info("检测结果对比完成，output=%s", output_path)


if __name__ == "__main__":
    main()
