import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path


RUN_STAMP = "202607201851"
RANDOM_SEED = 20260720
ROW_COUNT = 1000
CORRECT_COUNT = 700
OUTPUT_DIR = Path(__file__).resolve().parents[1]

FAMILY_NAMES = [
    "赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈",
    "褚", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "尤", "许",
    "何", "吕", "施", "张", "孔", "曹", "严", "华", "金", "魏",
]
GIVEN_CHARS = list("明华芳娜敏静强磊军洋勇艳杰娟涛超秀慧鹏飞玲霞瑞晨宇宁")
PROVINCES = ["北京市", "上海市", "广东省", "浙江省", "江苏省", "湖北省", "四川省", "山东省"]
CITIES = ["朝阳区", "浦东新区", "天河区", "西湖区", "姑苏区", "武昌区", "锦江区", "历下区"]
STREETS = ["建设路", "人民路", "幸福路", "解放路", "和平路", "科技路", "文昌路", "青年路"]
AREA_CODES = ["110101", "310115", "440106", "330106", "320508", "420106", "510104", "370102"]
EMAIL_DOMAINS = ["example.com", "mail.test", "demo.cn", "person.org", "data.cn"]
ID_CARD_WEIGHTS = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
ID_CARD_CHECK_CODES = "10X98765432"


def build_name(index):
    family = FAMILY_NAMES[index % len(FAMILY_NAMES)]
    first = GIVEN_CHARS[(index * 3) % len(GIVEN_CHARS)]
    second = GIVEN_CHARS[(index * 7 + 5) % len(GIVEN_CHARS)]
    return family + first + second


def birth_date_for_age(age, index):
    year = 2026 - age
    month = index % 12 + 1
    day = index % 27 + 1
    return date(year, month, day)


def build_id_card(age, index):
    area = AREA_CODES[index % len(AREA_CODES)]
    birthday = birth_date_for_age(age, index).strftime("%Y%m%d")
    sequence = "{:03d}".format(index % 999 + 1)
    body = area + birthday + sequence
    total = sum(int(body[pos]) * ID_CARD_WEIGHTS[pos] for pos in range(17))
    return body + ID_CARD_CHECK_CODES[total % 11]


def build_phone(index):
    prefixes = ["139", "138", "137", "136", "135", "158", "186", "188", "199"]
    return prefixes[index % len(prefixes)] + "{:08d}".format(20000000 + index)


def build_address(index):
    province = PROVINCES[index % len(PROVINCES)]
    city = CITIES[index % len(CITIES)]
    street = STREETS[index % len(STREETS)]
    return "{}{}{}{}号{}室".format(province, city, street, index % 300 + 1, index % 28 + 101)


def build_email(name, index):
    return "person{:04d}@{}".format(index, EMAIL_DOMAINS[index % len(EMAIL_DOMAINS)])


def base_record(index):
    age = 18 + (index * 11) % 63
    name = build_name(index)
    return {
        "name": name,
        "age": age,
        "phone": build_phone(index),
        "id_card": build_id_card(age, index),
        "email": build_email(name, index),
        "home_address": build_address(index),
    }


def break_name(record, index):
    record["name"] = ["", "A12", "测试用户###", "张三三三三三三三"][index % 4]


def break_age(record, index):
    record["age"] = [-5, 3, 151, 999][index % 4]


def break_phone(record, index):
    record["phone"] = ["1234567890", "20900000000", "1380000ABCD", ""][index % 4]


def break_id_card(record, index):
    bad_values = [
        record["id_card"][:-1] + ("0" if record["id_card"][-1] != "0" else "1"),
        record["id_card"][:15],
        "ABCDEFGHIJKLMNO123",
        "",
    ]
    record["id_card"] = bad_values[index % 4]


def break_email(record, index):
    record["email"] = ["person.example.com", "person@", "@demo.cn", "person 001@example.com"][index % 4]


def break_address(record, index):
    record["home_address"] = ["", "未知", "123", "火星基地A区"][index % 4]


ERROR_MUTATORS = [
    ("name", break_name),
    ("age", break_age),
    ("phone", break_phone),
    ("id_card", break_id_card),
    ("email", break_email),
    ("home_address", break_address),
]


def add_errors(record, index):
    error_columns = []
    start = index % len(ERROR_MUTATORS)
    count = 1 + (index % 3)
    for offset in range(count):
        column, mutator = ERROR_MUTATORS[(start + offset * 2) % len(ERROR_MUTATORS)]
        if column not in error_columns:
            mutator(record, index + offset)
            error_columns.append(column)
    return error_columns


def sql_string(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_value(record):
    return "({}, {}, {}, {}, {}, {})".format(
        sql_string(record["name"]),
        int(record["age"]),
        sql_string(record["phone"]),
        sql_string(record["id_card"]),
        sql_string(record["email"]),
        sql_string(record["home_address"]),
    )


def truth_key(record):
    return "\t".join(str(record[column]) for column in [
        "name", "age", "phone", "id_card", "email", "home_address"
    ])


def write_sql(records):
    sql_dir = OUTPUT_DIR / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    path = sql_dir / "person_info_create_insert_{}.sql".format(RUN_STAMP)
    lines = [
        "-- 人员基本信息测试表建表与造数脚本，数据量 1000 行，其中 700 行正确、300 行存在字段错误。",
        "CREATE DATABASE IF NOT EXISTS dw;",
        "DROP TABLE IF EXISTS dw.person_info;",
        "CREATE TABLE dw.person_info (",
        "    name STRING COMMENT '姓名',",
        "    age INT COMMENT '年龄',",
        "    phone STRING COMMENT '手机号',",
        "    id_card STRING COMMENT '身份证',",
        "    email STRING COMMENT '邮箱',",
        "    home_address STRING COMMENT '家庭地址'",
        ")",
        "USING ORC;",
        "",
    ]
    chunk_size = 200
    for start in range(0, len(records), chunk_size):
        chunk = records[start:start + chunk_size]
        lines.append("INSERT INTO dw.person_info VALUES")
        for index, item in enumerate(chunk):
            suffix = ";" if index == len(chunk) - 1 else ","
            lines.append("    {}{}".format(sql_value(item["record"]), suffix))
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")
    return path


def write_csv_and_truth(records):
    data_dir = OUTPUT_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    data_path = data_dir / "person_info_data_{}.csv".format(RUN_STAMP)
    truth_path = data_dir / "person_info_ground_truth_{}.csv".format(RUN_STAMP)
    truth_json_path = data_dir / "person_info_ground_truth_{}.json".format(RUN_STAMP)
    fieldnames = ["name", "age", "phone", "id_card", "email", "home_address"]
    with data_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for item in records:
            writer.writerow(item["record"])
    with truth_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "row_no", "is_error", "error_columns", "name", "age",
            "phone", "id_card", "email", "home_address"
        ])
        writer.writeheader()
        for item in records:
            row = {
                "row_no": item["row_no"],
                "is_error": 1 if item["error_columns"] else 0,
                "error_columns": ",".join(item["error_columns"]),
            }
            row.update(item["record"])
            writer.writerow(row)
    truth_payload = {
        "table": "dw.person_info",
        "rowCount": ROW_COUNT,
        "correctRowCount": CORRECT_COUNT,
        "errorRowCount": ROW_COUNT - CORRECT_COUNT,
        "columns": fieldnames,
        "rows": [
            {
                "rowNo": item["row_no"],
                "isError": bool(item["error_columns"]),
                "errorColumns": item["error_columns"],
                "key": truth_key(item["record"]),
                "values": item["record"],
            }
            for item in records
        ],
    }
    truth_json_path.write_text(
        json.dumps(truth_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )
    return data_path, truth_path, truth_json_path


def main():
    random.seed(RANDOM_SEED)
    flags = [False] * CORRECT_COUNT + [True] * (ROW_COUNT - CORRECT_COUNT)
    random.shuffle(flags)
    records = []
    for index, should_break in enumerate(flags, start=1):
        record = base_record(index)
        error_columns = add_errors(record, index) if should_break else []
        records.append({
            "row_no": index,
            "record": record,
            "error_columns": error_columns,
        })
    sql_path = write_sql(records)
    data_path, truth_path, truth_json_path = write_csv_and_truth(records)
    print(json.dumps({
        "sql": str(sql_path),
        "data": str(data_path),
        "truthCsv": str(truth_path),
        "truthJson": str(truth_json_path),
        "rowCount": len(records),
        "correctRowCount": sum(1 for item in records if not item["error_columns"]),
        "errorRowCount": sum(1 for item in records if item["error_columns"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
