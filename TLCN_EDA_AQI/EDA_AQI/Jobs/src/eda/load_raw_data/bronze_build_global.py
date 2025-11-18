#!/usr/bin/env python3
import os
import io
import json
import argparse
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from dotenv import load_dotenv
from minio import Minio

# =============================
# 0) Load ENV & constants
# =============================
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

MINIO_HOST   = os.environ.get("MINIO_HOST", "localhost:9004")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "air-quality")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

# nơi sẽ đặt file tổng
GLOBAL_PREFIX = "openmeteo/global"

# =============================
# 1) Helpers
# =============================
def get_minio_client() -> Minio:
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def list_all_daily_objects_for_year(client: Minio, year: int) -> List[str]:
    """
    Liệt kê tất cả object daily của 1 năm.
    Cấu trúc daily hiện tại: openmeteo/{year}/{month}/{day}/openmeteo_{YYYY_MM_DD}.csv
    """
    prefix = f"openmeteo/{year}/"
    objects = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
    keys = []
    for obj in objects:
        # chỉ lấy .csv
        if obj.object_name.endswith(".csv"):
            # loại trừ trường hợp mình đã có file global mà đặt cùng prefix
            if not obj.object_name.startswith(f"{GLOBAL_PREFIX}/"):
                keys.append(obj.object_name)
    return keys


def read_csv_from_minio(client: Minio, object_name: str) -> pd.DataFrame:
    """
    Đọc 1 CSV từ MinIO về DataFrame
    """
    resp = client.get_object(MINIO_BUCKET, object_name)
    data = resp.read()
    df = pd.read_csv(io.BytesIO(data))
    return df


def write_csv_to_minio(client: Minio, df: pd.DataFrame, year: int):
    """
    Ghi file tổng năm lên MinIO.
    Đường dẫn: openmeteo/global/{year}/openmeteo_{year}.csv
    """
    ensure_bucket(client, MINIO_BUCKET)
    object_name = f"{GLOBAL_PREFIX}/{year}/openmeteo_{year}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    csv_stream = io.BytesIO(csv_bytes)

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=csv_stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )
    print(f"✅ Uploaded yearly file: s3://{MINIO_BUCKET}/{object_name} (rows={len(df)})")


def detect_years_from_bucket(client: Minio) -> List[int]:
    """
    Quét bucket để đoán ra những năm đang có dữ liệu daily.
    Ví dụ thấy openmeteo/2024/..., openmeteo/2025/... thì trả về [2024, 2025]
    """
    objects = client.list_objects(MINIO_BUCKET, prefix="openmeteo/", recursive=False)
    years = set()
    for obj in objects:
        # obj.object_name có thể là "openmeteo/2024/" hoặc "openmeteo/global/"
        parts = obj.object_name.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "openmeteo":
            # parts[1] có thể là "2024" hoặc "global"
            if parts[1].isdigit():
                years.add(int(parts[1]))
    return sorted(years)


# =============================
# 2) Build global per year
# =============================
def build_year(client: Minio, year: int):
    print(f"\n🛠 Building yearly CSV for {year} ...")

    daily_keys = list_all_daily_objects_for_year(client, year)
    if not daily_keys:
        print(f"  ⚠️ No daily files found for year {year}")
        return

    dfs = []
    for key in daily_keys:
        try:
            df = read_csv_from_minio(client, key)
            dfs.append(df)
            print(f"  + loaded {key} (rows={len(df)})")
        except Exception as e:
            print(f"  ⚠️ Failed to read {key}: {e}")

    if not dfs:
        print(f"  ⚠️ No dataframes loaded for year {year}")
        return

    # gộp toàn bộ
    full_df = pd.concat(dfs, ignore_index=True)

    # nếu có cột location_key và ts_utc thì drop trùng theo 2 cột này
    if "ts_utc" in full_df.columns and "location_key" in full_df.columns:
        full_df = full_df.drop_duplicates(subset=["ts_utc", "location_key"], keep="last")
    else:
        full_df = full_df.drop_duplicates()

    # sắp xếp theo thời gian nếu có
    if "ts_utc" in full_df.columns:
        full_df = full_df.sort_values("ts_utc").reset_index(drop=True)

    # ghi lên MinIO
    write_csv_to_minio(client, full_df, year)


# =============================
# 3) CLI
# =============================
def main():
    parser = argparse.ArgumentParser(
        description="Build yearly global CSV from daily Open-Meteo bronze files in MinIO"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Year to build (e.g. 2024)")
    group.add_argument("--all", action="store_true", help="Build for all years detected in bucket")
    args = parser.parse_args()

    client = get_minio_client()

    if args.year:
        build_year(client, args.year)
    else:
        years = detect_years_from_bucket(client)
        if not years:
            print("❌ No years detected under 'openmeteo/' in the bucket.")
            return
        for y in years:
            build_year(client, y)


if __name__ == "__main__":
    main()
