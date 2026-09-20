#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""배치 파일 유틸리티 (고속 처리)"""

import csv
import hashlib
import json
import sys


def cmd_sha256(file_path):
    """파일의 SHA256 해시 계산"""
    with open(file_path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def cmd_json_get(file_path, key):
    """JSON 파일에서 키 값 추출"""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    value = data
    for part in key.split("."):
        value = value[part]

    return str(value)


def cmd_csv_max_date(csv_path, datetime_col="datetime"):
    """CSV에서 최신 YYYYMMDD 날짜 추출"""
    max_ymd = None

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt_str = row.get(datetime_col, "")
            if not dt_str:
                continue

            ymd = dt_str.split("T")[0]
            if len(ymd) == 8 and ymd.isdigit():
                if max_ymd is None or ymd > max_ymd:
                    max_ymd = ymd

    if not max_ymd:
        raise ValueError("No valid date found")

    return max_ymd


def main(argv):
    if len(argv) < 2:
        print("Usage: batch_utils.py <command> [args...]", file=sys.stderr)
        print("Commands:", file=sys.stderr)
        print("  sha256 <file>", file=sys.stderr)
        print("  json-get <file> <key>", file=sys.stderr)
        print("  csv-max-date <file> [datetime_col]", file=sys.stderr)
        return 1

    cmd = argv[1]

    try:
        if cmd == "sha256":
            print(cmd_sha256(argv[2]))
        elif cmd == "json-get":
            print(cmd_json_get(argv[2], argv[3]))
        elif cmd == "csv-max-date":
            datetime_col = argv[3] if len(argv) > 3 else "datetime"
            print(cmd_csv_max_date(argv[2], datetime_col))
        else:
            print(f"Unknown command: {cmd}", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
