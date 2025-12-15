# /opt/EDA_AQI/Jobs/src/eda/pre-processing/data_preprocessing.py
# hoặc /opt/EDA_AQI/Jobs/src/eda/silver_clean_global.py
# (tùy bạn đang dùng path nào trong DAG, nội dung file là như nhau)

import os
import argparse
import tempfile
import time
from io import BytesIO  # để đây phòng khi cần dùng sau
import gc

import numpy as np
import pandas as pd
from minio import Minio
from scipy import stats
from urllib3.exceptions import ProtocolError


# =============================
# CẤU HÌNH MINIO
# =============================
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "air-quality")
MINIO_CLEAN_BUCKET = os.getenv("MINIO_CLEAN_BUCKET", "air-quality-clean")


# =============================
# HÀM TIỆN ÍCH MINIO
# =============================
def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,  # nếu sau này MINIO_SECURE=true thì đọc từ env rồi sửa chỗ này
    )


def list_years_and_files(client, bucket=MINIO_BUCKET, prefix="openmeteo/global/"):
    """
    Liệt kê các file CSV trong MinIO theo từng năm.
    Cấu trúc path giả định: openmeteo/global/<year>/<something>.csv
    """
    all_objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    files_by_year = {}
    years = set()

    for obj in all_objects:
        path = obj.object_name  # vd: openmeteo/global/2025/file.csv
        parts = path.split("/")
        if len(parts) >= 4 and path.endswith(".csv"):
            year = parts[2]
            years.add(year)
            files_by_year.setdefault(year, []).append(path)

    return sorted(years), files_by_year


# =============================
# HÀM ĐỌC CSV CÓ RETRY (STREAM, KHÔNG ĐỌC FULL VÀO RAM)
# =============================
def iter_csv_chunks_from_minio(
    client,
    bucket,
    path,
    chunksize=50_000,      # GIẢM chunksize để bớt tốn RAM
    max_retries=3,
):
    """
    Đọc CSV từ MinIO với retry, dùng streaming thay vì đọc toàn bộ vào RAM.
    Trả về các chunk DataFrame (generator).
    """
    attempt = 1
    while True:
        try:
            print(f"[INFO] Đọc {bucket}/{path}, attempt {attempt}/{max_retries}")
            resp = client.get_object(bucket, path)

            try:
                if chunksize:
                    for chunk in pd.read_csv(resp, chunksize=chunksize):
                        yield chunk
                else:
                    yield pd.read_csv(resp)
            finally:
                resp.close()
                resp.release_conn()

            break  # thành công → thoát retry loop

        except ProtocolError as e:
            print(
                f"[WARN] ProtocolError khi đọc {bucket}/{path} "
                f"(attempt {attempt}/{max_retries}): {e}"
            )
            attempt += 1
            if attempt > max_retries:
                print("[ERROR] Hết số lần retry → raise lỗi.")
                raise
            time.sleep(3)

        except Exception as e:
            print(f"[ERROR] Lỗi khi đọc {bucket}/{path}: {e}")
            raise


def upload_file_to_minio(client, bucket, path, local_path):
    size = os.path.getsize(local_path)
    with open(local_path, "rb") as f:
        client.put_object(
            bucket,
            path,
            data=f,
            length=size,
            content_type="text/csv",
        )


# =============================
# EDA BASED ON FIRST CHUNK (NHƯNG CHỈ SAMPLE)
# =============================
def run_eda(df, z_threshold=7.0, null_threshold=40, max_rows_for_eda=50_000):
    """
    Chạy EDA trên sample để tránh ăn RAM khi chunk rất to.
    """
    # Nếu df quá to thì lấy sample
    if len(df) > max_rows_for_eda:
        print(
            f"[INFO] EDA sample {max_rows_for_eda} / {len(df)} rows "
            f"để tiết kiệm RAM"
        )
        df_eda = df.sample(n=max_rows_for_eda, random_state=42)
    else:
        df_eda = df

    report = {}
    report["shape_before"] = df.shape

    # Các cột toàn null
    all_null_cols = df_eda.columns[df_eda.isna().all()].tolist()
    report["all_null_columns"] = all_null_cols

    # % null theo cột (trên sample, nhưng vẫn phản ánh tương đối)
    null_percent = (df_eda.isnull().mean() * 100).to_dict()
    report["null_percent"] = null_percent

    # Cột có % null > ngưỡng
    cols_to_drop_by_null = [c for c, p in null_percent.items() if p > null_threshold]
    report["cols_dropped_by_null_threshold"] = cols_to_drop_by_null

    # Tổng hợp cột drop
    dropped_cols_info = {
        c: null_percent.get(c, 0)
        for c in set(all_null_cols + cols_to_drop_by_null)
    }
    report["dropped_columns_with_null_percent"] = dropped_cols_info

    # DataFrame sau khi drop cột "quá bẩn"
    df_clean = df_eda.drop(columns=list(dropped_cols_info.keys()), errors="ignore")

    # Describe
    try:
        report["describe"] = df_clean.describe(include="all").to_dict()
    except Exception:
        report["describe"] = {}

    # Phân tích outlier
    numeric_df = df_clean.select_dtypes(include=[np.number])
    report["numeric_columns"] = numeric_df.columns.tolist()

    outlier_info = {}
    if not numeric_df.empty:
        valid_cols = numeric_df.loc[:, numeric_df.std(skipna=True) > 0]
        if not valid_cols.empty:
            z = np.abs(stats.zscore(valid_cols, nan_policy="omit"))
            z_df = pd.DataFrame(z, columns=valid_cols.columns, index=valid_cols.index)

            rows_with_outlier = (z_df >= z_threshold).any(axis=1)
            outlier_counts_by_col = (z_df >= z_threshold).sum(axis=0).to_dict()

            outlier_info["total_outlier_rows"] = int(rows_with_outlier.sum())
            outlier_info["outlier_counts_by_column"] = {
                k: int(v) for k, v in outlier_counts_by_col.items()
            }
            outlier_info["outlier_samples"] = (
                df_clean.loc[rows_with_outlier].head(20).to_dict(orient="records")
            )
    report["outlier_analysis"] = outlier_info

    # Corr
    try:
        report["correlation"] = df_clean.corr().to_dict()
    except Exception:
        report["correlation"] = {}

    report["shape_after_drop_for_eda"] = df_clean.shape
    return report, df_clean


def save_eda_report(report, file_choice, report_dir="eda_reports"):
    os.makedirs(report_dir, exist_ok=True)
    base = os.path.basename(file_choice).replace(".csv", "")
    out_path = os.path.join(report_dir, f"{base}_eda_report.xlsx")

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        pd.DataFrame(report.get("describe", {})).T.to_excel(
            writer, sheet_name="describe"
        )
        pd.DataFrame.from_dict(
            report.get("null_percent", {}), orient="index", columns=["% Null"]
        ).to_excel(writer, sheet_name="null_percent")
        pd.DataFrame.from_dict(
            report.get("dropped_columns_with_null_percent", {}),
            orient="index",
            columns=["% Null"],
        ).to_excel(writer, sheet_name="dropped_columns")
        outlier_counts = report.get("outlier_analysis", {}).get(
            "outlier_counts_by_column", {}
        )
        pd.DataFrame.from_dict(
            outlier_counts,
            orient="index",
            columns=["Outliers"],
        ).to_excel(writer, sheet_name="outliers")

    return out_path


# =============================
# CLEANING PER CHUNK
# =============================
def clean_chunk(chunk, cols_to_drop, z_threshold=7.0):
    """
    - Drop các cột cần loại bỏ (null nhiều, metadata không cần thiết)
    - Loại outlier theo Z-score
    - Drop tất cả hàng còn null
    """
    chunk = chunk.drop(columns=list(cols_to_drop), errors="ignore")

    numeric_cols = chunk.select_dtypes(include=[np.number]).columns
    valid_cols = numeric_cols[chunk[numeric_cols].std(skipna=True) > 0]

    if len(valid_cols) > 0:
        z = np.abs(stats.zscore(chunk[valid_cols], nan_policy="omit"))
        rows_with_outlier = (z >= z_threshold).any(axis=1)
        chunk = chunk.loc[~rows_with_outlier]

    return chunk.dropna(how="any")


# =============================
# PROCESS 1 FILE (CHUNKED)
# =============================
def process_file_chunked(
    client,
    file_path,
    z_threshold=7.0,
    null_threshold=40,
    bucket=MINIO_BUCKET,
    clean_bucket=MINIO_CLEAN_BUCKET,
):
    print(f"=== Xử lý file: {file_path} ===")

    chunks = iter_csv_chunks_from_minio(
        client, bucket, file_path, chunksize=50_000  # giảm thêm cho chắc
    )

    try:
        first_chunk = next(chunks)
    except StopIteration:
        print("  - File rỗng, bỏ qua.")
        return

    extra_drop_cols = ["date_utc", "latitude", "longitude", "_ingested_at", "year_utc"]
    first_chunk = first_chunk.drop(columns=extra_drop_cols, errors="ignore")

    report, _ = run_eda(first_chunk.copy(), z_threshold, null_threshold)
    cols_to_drop = (
        set(report.get("all_null_columns", []))
        | set(report.get("cols_dropped_by_null_threshold", []))
        | set(extra_drop_cols)
    )

    report_path = save_eda_report(report, file_path)
    print(f"  - Đã lưu EDA report: {report_path}")

    fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    first = True

    cleaned = clean_chunk(first_chunk, cols_to_drop, z_threshold)
    if not cleaned.empty:
        cleaned.to_csv(tmp_path, index=False, mode="w", header=True)
        first = False

    # Giải phóng bớt bộ nhớ trung gian
    del first_chunk
    gc.collect()

    for i, chunk in enumerate(chunks, start=1):
        print(f"  - Processing chunk #{i}")
        chunk = chunk.drop(columns=extra_drop_cols, errors="ignore")
        cleaned = clean_chunk(chunk, cols_to_drop, z_threshold)
        if not cleaned.empty:
            cleaned.to_csv(tmp_path, index=False, mode="a", header=first)
            first = False
        del chunk, cleaned
        gc.collect()

    if first:
        print("  - Không có bản ghi nào sau khi làm sạch, bỏ qua upload.")
        os.remove(tmp_path)
        print(f"=== DONE (empty after cleaning): {file_path} ===\n")
        return

    if not client.bucket_exists(clean_bucket):
        client.make_bucket(clean_bucket)

    upload_file_to_minio(client, clean_bucket, file_path, tmp_path)
    os.remove(tmp_path)
    print(f"=== DONE file: {file_path} ===\n")


# =============================
# PROCESS 1 YEAR
# =============================
def process_year(year, z_threshold=7.0, null_threshold=40):
    client = get_minio_client()

    years, files_by_year = list_years_and_files(client, MINIO_BUCKET)

    if year not in files_by_year:
        print(f"⚠ Không tìm thấy file cho năm {year}")
        return

    files = files_by_year[year]
    print(f"▶ Bắt đầu xử lý {year}, tổng {len(files)} file")

    for f in files:
        process_file_chunked(
            client=client,
            file_path=f,
            z_threshold=z_threshold,
            null_threshold=null_threshold,
        )
        gc.collect()

    print(f"✅ Hoàn thành xử lý năm {year}")


# =============================
# MAIN
# =============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    args = parser.parse_args()

    process_year(args.year)
