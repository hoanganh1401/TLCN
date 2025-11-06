# src/etl/bronze_build_global.py

import os
import io
import sys
import json
from datetime import datetime
from typing import List

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

# ========= 0) ENV =========
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

MINIO_HOST   = os.environ.get("MINIO_HOST", "localhost:9004")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "air-quality")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

# Nơi lưu file tổng (Parquet)
GLOBAL_PREFIX = "openmeteo/_global"
GLOBAL_SNAPSHOT_NAME = lambda ts: f"{GLOBAL_PREFIX}/all_cities_all_days_{ts}.parquet"
GLOBAL_LATEST_NAME   = f"{GLOBAL_PREFIX}/all_cities_all_days_latest.parquet"

# ========= 1) MinIO client =========
def get_minio() -> Minio:
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE,
    )

def ensure_bucket(cli: Minio, bucket: str):
    if not cli.bucket_exists(bucket):
        cli.make_bucket(bucket)

# ========= 2) Liệt kê tất cả object CSV theo cấu trúc Bronze =========
def list_bronze_csv_objects(cli: Minio) -> List[str]:
    """
    Trả về danh sách object_name kiểu:
    openmeteo/<location_key>/year=YYYY/month=MM/day=DD/openmeteo_<key>_<YYYY-MM-DD>.csv
    """
    object_names = []
    # Duyệt prefix 'openmeteo/' toàn bộ
    for obj in cli.list_objects(MINIO_BUCKET, prefix="openmeteo/", recursive=True):
        name = obj.object_name
        # Lọc folder _global (không phải dữ liệu raw theo location)
        if name.startswith(f"{GLOBAL_PREFIX}/"):
            continue
        # Lấy các file csv raw
        if name.endswith(".csv") and "/year=" in name and "/month=" in name and "/day=" in name:
            object_names.append(name)
    return object_names

# ========= 3) Đọc toàn bộ CSV -> concat =========
def read_all_csv_to_df(cli: Minio, object_names: List[str]) -> pd.DataFrame:
    """
    Đọc lần lượt từng CSV vào pandas và gộp (concat). Giữ schema như ở Bronze CSV.
    Thêm cột 'source_object' để trace ngược nếu cần.
    """
    frames = []
    for i, name in enumerate(object_names, start=1):
        try:
            resp = cli.get_object(MINIO_BUCKET, name)
            data = resp.read()  # đọc toàn bộ vào memory
            resp.close(); resp.release_conn()
            df = pd.read_csv(io.BytesIO(data))
            # Chuẩn hoá kiểu ngày giờ (nếu chưa đúng)
            if "ts_utc" in df.columns:
                df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
            if "date_utc" in df.columns:
                # một số pandas có thể đọc thành object -> ép date
                df["date_utc"] = pd.to_datetime(df["date_utc"], errors="coerce").dt.date
            # gắn nguồn
            df["source_object"] = name
            frames.append(df)
            if i % 500 == 0:
                print(f"  ...loaded {i} files")
        except S3Error as e:
            print(f"Skip {name} due to S3Error: {e}")
        except Exception as e:
            print(f"Skip {name} due to error: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

# ========= 4) Ghi Parquet snapshot + latest =========
def save_parquet(cli: Minio, df: pd.DataFrame):
    # Đảm bảo có thư mục _global (MinIO không cần tạo folder, nhưng tạo file placeholder là được)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snap_key = GLOBAL_SNAPSHOT_NAME(ts)
    latest_key = GLOBAL_LATEST_NAME

    # Lưu Parquet (cần pyarrow hoặc fastparquet)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: cần 'pyarrow'. Hãy cài: pip install pyarrow")
        sys.exit(1)

    # Convert pandas -> parquet bytes
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    payload = sink.getvalue()

    # Upload snapshot
    cli.put_object(
        MINIO_BUCKET, snap_key, io.BytesIO(payload), length=len(payload),
        content_type="application/octet-stream"
    )
    print(f"✅ Snapshot uploaded: s3://{MINIO_BUCKET}/{snap_key}")

    # Upload/overwrite latest
    cli.put_object(
        MINIO_BUCKET, latest_key, io.BytesIO(payload), length=len(payload),
        content_type="application/octet-stream"
    )
    print(f"✅ Latest uploaded:   s3://{MINIO_BUCKET}/{latest_key}")

def main():
    cli = get_minio()
    ensure_bucket(cli, MINIO_BUCKET)

    print("🔎 Listing Bronze CSV objects ...")
    objects = list_bronze_csv_objects(cli)
    print(f"Found {len(objects)} CSV files")

    if not objects:
        print("No data to build global file.")
        sys.exit(0)

    print("📚 Reading & concatenating ...")
    df = read_all_csv_to_df(cli, objects)

    if df.empty:
        print("No rows after read; abort.")
        sys.exit(0)

    # Optional: sắp xếp để file nhất quán
    sort_cols = [c for c in ["date_utc", "ts_utc", "location_key"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    print(f"🚀 Saving global parquet with {len(df)} rows ...")
    save_parquet(cli, df)
    print("DONE.")
    sys.exit(0)

if __name__ == "__main__":
    main()
