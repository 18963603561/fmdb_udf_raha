import argparse
import json
from pathlib import Path

import xlrd
from xlutils.copy import copy as copy_workbook


BUSINESS_COLUMNS = ["name", "age", "phone", "id_card", "email", "home_address"]


def cell_text(value):
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def truth_key(values):
    return "\t".join(cell_text(values[column]) for column in BUSINESS_COLUMNS)


def load_truth(path):
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    result = {}
    for row in payload["rows"]:
        result[row["key"]] = row["errorColumns"]
    return result


def auto_label(input_path, truth_path, output_path, summary_path):
    truth = load_truth(truth_path)
    read_book = xlrd.open_workbook(str(input_path), formatting_info=True)
    sheet = read_book.sheet_by_name("标注数据")
    headers = [cell_text(value) for value in sheet.row_values(0)]
    column_indexes = {name: headers.index(name) for name in headers}
    required = BUSINESS_COLUMNS + ["_row_label", "_error_columns", "_comment"]
    for name in required:
        if name not in column_indexes:
            raise ValueError("标注模板缺少必要字段：{}".format(name))

    write_book = copy_workbook(read_book)
    write_sheet = write_book.get_sheet(read_book.sheet_names().index("标注数据"))
    labeled_count = 0
    error_row_count = 0
    unmatched_rows = []
    for row_index in range(1, sheet.nrows):
        values = {
            column: cell_text(sheet.cell_value(row_index, column_indexes[column]))
            for column in BUSINESS_COLUMNS
        }
        key = truth_key(values)
        if key not in truth:
            unmatched_rows.append(row_index + 1)
            continue
        error_columns = truth[key]
        is_error = bool(error_columns)
        write_sheet.write(row_index, column_indexes["_row_label"], 1 if is_error else 0)
        write_sheet.write(row_index, column_indexes["_error_columns"], ",".join(error_columns))
        write_sheet.write(
            row_index,
            column_indexes["_comment"],
            "自动标注：{}".format(",".join(error_columns) if is_error else "正常"),
        )
        labeled_count += 1
        if is_error:
            error_row_count += 1

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_book.save(str(output_path))
    summary = {
        "input": str(input_path),
        "output": str(output_path),
        "labeledCount": labeled_count,
        "errorRowCount": error_row_count,
        "correctRowCount": labeled_count - error_row_count,
        "unmatchedRows": unmatched_rows,
    }
    Path(summary_path).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description="自动标注 Raha 采样 Excel")
    parser.add_argument("--input", required=True)
    parser.add_argument("--truth", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary", required=True)
    args = parser.parse_args()
    auto_label(Path(args.input), Path(args.truth), Path(args.output), Path(args.summary))


if __name__ == "__main__":
    main()
