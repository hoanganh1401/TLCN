#!/usr/bin/env python3
import os
import io
import json
import argparse
import tempfile
import time
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from dotenv import load_dotenv
from minio import Minio

# =============================
# 0) Load ENV & constants
# =============================
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
dotenv_path = os.path.join(ROOT_DIR, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

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
            # loại trừ file global nếu vô tình nằm cùng prefix
            if not obj.object_name.startswith(f"{GLOBAL_PREFIX}/"):
                keys.append(obj.object_name)
    return keys


def upload_file_to_minio_from_path(client: Minio, year: int, local_path: str):
    """
    Upload file CSV local (đã ghép toàn bộ năm) lên MinIO.
    Đường dẫn: openmeteo/global/{year}/openmeteo_{year}.csv
    """
    ensure_bucket(client, MINIO_BUCKET)
    object_name = f"{GLOBAL_PREFIX}/{year}/openmeteo_{year}.csv"

    size = os.path.getsize(local_path)
    with open(local_path, "rb") as f:
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=f,
            length=size,
            content_type="text/csv",
        )

    # Đếm nhanh số dòng (trừ header) chỉ để log (có thể bỏ nếu sợ tốn I/O)
    try:
        import csv
        with open(local_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = sum(1 for _ in reader) - 1  # trừ header
    except Exception:
        rows = "unknown"

    print(f"✅ Uploaded yearly file: s3://{MINIO_BUCKET}/{object_name} (rows={rows})")


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
# 2) Build global per year (STREAMING)
# =============================
def build_year(client: Minio, year: int, chunksize: int = 100_000):
    print(f"\n🛠 Building yearly CSV for {year} ...")

    daily_keys = list_all_daily_objects_for_year(client, year)
    if not daily_keys:
        print(f"  ⚠️ No daily files found for year {year}")
        return

    # Tạo file tạm để ghép tất cả ngày
    fd, tmp_path = tempfile.mkstemp(suffix=f"_{year}.csv")
    os.close(fd)
    first = True

    for key in sorted(daily_keys):
        print(f"  + loading {key}")
        try:
            resp = client.get_object(MINIO_BUCKET, key)
        except Exception as e:
            print(f"  ⚠️ Failed to open {key}: {e}")
            continue

        try:
            # Đọc từng chunk từ CSV (streaming, không load full file vào RAM)
            for chunk in pd.read_csv(resp, chunksize=chunksize):
                # Xử lý duplicate trong chunk
                if "ts_utc" in chunk.columns and "location_key" in chunk.columns:
                    chunk = chunk.drop_duplicates(
                        subset=["ts_utc", "location_key"], keep="last"
                    )
                else:
                    chunk = chunk.drop_duplicates()

                # (Tuỳ chọn) sắp xếp theo thời gian trong phạm vi chunk
                if "ts_utc" in chunk.columns:
                    chunk = chunk.sort_values("ts_utc")

                # Append ra file tạm
                if not chunk.empty:
                    chunk.to_csv(
                        tmp_path,
                        index=False,
                        mode="w" if first else "a",
                        header=first,
                    )
                    first = False
        except Exception as e:
            print(f"  ⚠️ Failed to read {key}: {e}")
        finally:
            try:
                resp.close()
                resp.release_conn()
            except Exception:
                pass

    if first:
        # Chưa ghi được dòng nào → không có dữ liệu
        print(f"  ⚠️ No data written for year {year}, skip upload.")
        os.remove(tmp_path)
        return

    # Upload file tạm lên MinIO dưới dạng yearly global
    upload_file_to_minio_from_path(client, year, tmp_path)
    os.remove(tmp_path)
    print(f"✅ Finished building yearly CSV for {year}")


# =============================
# 3) CLI
# =============================
def main():
    parser = argparse.ArgumentParser(
        description="Build yearly global CSV from daily Open-Meteo bronze files in MinIO (streaming, low RAM)"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Year to build (e.g. 2024)")
    group.add_argument(
        "--all", action="store_true", help="Build for all years detected in bucket"
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Number of rows per chunk when reading daily CSVs",
    )
    args = parser.parse_args()

    client = get_minio_client()

    if args.year:
        build_year(client, args.year, chunksize=args.chunksize)
    else:
        years = detect_years_from_bucket(client)
        if not years:
            print("❌ No years detected under 'openmeteo/' in the bucket.")
            return
        for y in years:
            build_year(client, y, chunksize=args.chunksize)


if __name__ == "__main__":
    main()
