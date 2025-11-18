#!/usr/bin/env python3
import os
import io
import argparse
from typing import List

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

# prefix dùng chung với file yearly bạn đã tạo
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


def read_csv_from_minio(client: Minio, object_name: str) -> pd.DataFrame:
    """
    Đọc 1 CSV từ MinIO về DataFrame
    """
    resp = client.get_object(MINIO_BUCKET, object_name)
    data = resp.read()
    df = pd.read_csv(io.BytesIO(data))
    return df


def detect_years_with_yearly_files(client: Minio) -> List[int]:
    """
    Quét dưới GLOBAL_PREFIX để tìm các năm đã có file yearly.
    Cấu trúc: openmeteo/global/{year}/openmeteo_{year}.csv
    """
    objects = client.list_objects(
        bucket_name=MINIO_BUCKET,
        prefix=f"{GLOBAL_PREFIX}/",
        recursive=True
    )
    years = set()
    for obj in objects:
        # ví dụ: openmeteo/global/2024/openmeteo_2024.csv
        parts = obj.object_name.strip("/").split("/")
        if len(parts) >= 3 and parts[0] == "openmeteo" and parts[1] == "global":
            if parts[2].isdigit():
                years.add(int(parts[2]))
    return sorted(years)


def write_multi_year_csv_to_minio(client: Minio, df: pd.DataFrame, years: List[int]):
    """
    Ghi file tổng multi-year lên MinIO.
    Đường dẫn gợi ý: openmeteo/global/combined/openmeteo_{minyear}_{maxyear}.csv
    """
    ensure_bucket(client, MINIO_BUCKET)
    years_sorted = sorted(years)
    min_year, max_year = years_sorted[0], years_sorted[-1]

    # Bạn có thể đổi 'combined' thành tên khác nếu thích
    object_name = f"{GLOBAL_PREFIX}/combined/openmeteo_{min_year}_{max_year}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    csv_stream = io.BytesIO(csv_bytes)

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=csv_stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )
    print(f"✅ Uploaded multi-year file: s3://{MINIO_BUCKET}/{object_name} (rows={len(df)})")


# =============================
# 2) Build multi-year
# =============================
def build_multi_year(client: Minio, years: List[int]):
    years = sorted(list(set(years)))
    print(f"\n🛠 Building multi-year CSV for years: {years} ...")

    dfs = []
    for y in years:
        object_name = f"{GLOBAL_PREFIX}/{y}/openmeteo_{y}.csv"
        try:
            df_y = read_csv_from_minio(client, object_name)
            dfs.append(df_y)
            print(f"  + loaded {object_name} (rows={len(df_y)})")
        except Exception as e:
            print(f"  ⚠️ Failed to read yearly file for {y}: {e}")

    if not dfs:
        print("❌ No yearly files loaded. Abort.")
        return

    # Gộp các năm lại
    full_df = pd.concat(dfs, ignore_index=True)

    # Nếu có cột ts_utc và location_key thì drop trùng theo 2 cột này
    if "ts_utc" in full_df.columns and "location_key" in full_df.columns:
        full_df = full_df.drop_duplicates(subset=["ts_utc", "location_key"], keep="last")
    else:
        full_df = full_df.drop_duplicates()

    # Sắp xếp theo thời gian nếu có
    if "ts_utc" in full_df.columns:
        full_df = full_df.sort_values("ts_utc").reset_index(drop=True)

    # Ghi lên MinIO
    write_multi_year_csv_to_minio(client, full_df, years)


# =============================
# 3) CLI
# =============================
def main():
    parser = argparse.ArgumentParser(
        description="Build multi-year global CSV from existing yearly Open-Meteo files in MinIO"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--years",
        nargs="+",
        type=int,
        help="List of years to merge (e.g. --years 2024 2025)"
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Merge all years that already have yearly files under openmeteo/global/"
    )
    args = parser.parse_args()

    client = get_minio_client()

    if args.years:
        build_multi_year(client, args.years)
    else:
        years = detect_years_with_yearly_files(client)
        if not years:
            print("❌ No yearly files detected under 'openmeteo/global/' in the bucket.")
            return
        build_multi_year(client, years)


if __name__ == "__main__":
    main()
