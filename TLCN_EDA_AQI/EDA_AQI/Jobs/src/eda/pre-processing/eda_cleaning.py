import streamlit as st
from minio import Minio
from io import BytesIO
import pandas as pd
import numpy as np
from scipy import stats
import os

# =============================
# CẤU HÌNH MINIO
# =============================
#MINIO_HOST = "localhost:9004"
MINIO_HOST = "172.27.91.163:9004"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "air-quality"
MINIO_CLEAN_BUCKET = "air-quality-clean"

# =============================
# HÀM TIỆN ÍCH
# =============================
def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def list_years_and_files(client, bucket):
    all_objects = list(client.list_objects(bucket, prefix="openmeteo/global/", recursive=True))
    files_by_year = {}
    years = set()
    for obj in all_objects:
        path = obj.object_name
        parts = path.split("/")
        if len(parts) >= 4 and path.endswith(".csv"):
            year = parts[2]
            years.add(year)
            files_by_year.setdefault(year, []).append(path)
    return sorted(years), files_by_year

def load_csv_from_minio(client, bucket, path):
    response = client.get_object(bucket, path)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_csv(BytesIO(data))

# =============================
# EDA
# =============================
def run_eda(df, z_threshold=7.0, null_threshold=40):
    report = {}
    report['shape_before'] = df.shape

    all_null_cols = df.columns[df.isna().all()].tolist()
    report['all_null_columns'] = all_null_cols

    null_percent = (df.isnull().mean() * 100).to_dict()
    report['null_percent'] = null_percent

    cols_to_drop_by_null = [c for c, p in null_percent.items() if p > null_threshold]
    report['cols_dropped_by_null_threshold'] = cols_to_drop_by_null

    dropped_cols_info = {c: null_percent.get(c, 0) for c in set(all_null_cols + cols_to_drop_by_null)}
    report['dropped_columns_with_null_percent'] = dropped_cols_info

    df_clean = df.drop(columns=list(dropped_cols_info.keys()), errors='ignore')
    report['describe'] = df_clean.describe(include='all').to_dict()

    numeric_df = df_clean.select_dtypes(include=[np.number])
    report['numeric_columns'] = numeric_df.columns.tolist()
    outlier_info = {}

    if not numeric_df.empty:
        valid_cols = numeric_df.loc[:, numeric_df.std(skipna=True) > 0]
        if not valid_cols.empty:
            z = np.abs(stats.zscore(valid_cols, nan_policy='omit'))
            z_df = pd.DataFrame(z, columns=valid_cols.columns, index=valid_cols.index)
            rows_with_outlier = (z_df >= z_threshold).any(axis=1)
            outlier_rows_idx = z_df[rows_with_outlier].index.tolist()
            outlier_counts_by_col = (z_df >= z_threshold).sum(axis=0).to_dict()

            outlier_info['total_outlier_rows'] = int(rows_with_outlier.sum())
            outlier_info['outlier_counts_by_column'] = {k: int(v) for k, v in outlier_counts_by_col.items()}
            outlier_info['outlier_samples'] = df_clean.loc[outlier_rows_idx].head(20).to_dict(orient='records')
        else:
            outlier_info['note'] = 'Không có cột số hợp lệ để tính z-score.'
    else:
        outlier_info['note'] = 'Không có cột số để phân tích.'

    report['outlier_analysis'] = outlier_info

    try:
        report['correlation'] = df_clean.corr().to_dict()
    except Exception:
        report['correlation'] = {}

    report['shape_after_drop_for_eda'] = df_clean.shape
    return report, df_clean

# =============================
# CLEANING
# =============================
def run_cleaning(df, report, z_threshold=7.0, null_threshold=40):
    cols_to_drop = set(report.get('all_null_columns', []) + report.get('cols_dropped_by_null_threshold', []))
    extra_drop_cols = ['date_utc', 'latitude', 'longitude', '_ingested_at', 'year_utc']
    cols_to_drop.update(extra_drop_cols)

    df = df.drop(columns=list(cols_to_drop), errors='ignore')

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if numeric_cols:
        valid_cols = df[numeric_cols].loc[:, df[numeric_cols].std(skipna=True) > 0]
        if not valid_cols.empty:
            z = np.abs(stats.zscore(valid_cols, nan_policy='omit'))
            z_df = pd.DataFrame(z, columns=valid_cols.columns, index=valid_cols.index)
            rows_with_outlier = (z_df >= z_threshold).any(axis=1)
            df = df.loc[~rows_with_outlier]

    df = df.dropna(how='any')
    return df

# =============================
# STREAMLIT UI
# =============================
def run():
    st.set_page_config(page_title="EDA & Làm sạch dữ liệu", layout="wide")
    st.title("EDA & Làm sạch dữ liệu không khí từ MinIO")

    client = get_minio_client()

    try:
        if not client.bucket_exists(MINIO_BUCKET):
            st.error(f"Bucket '{MINIO_BUCKET}' không tồn tại!")
            return
        st.success(f"Kết nối thành công tới bucket: {MINIO_BUCKET}")
    except Exception as e:
        st.error(f"Lỗi kết nối MinIO: {e}")
        return

    # Chọn năm và file
    years, files_by_year = list_years_and_files(client, MINIO_BUCKET)
    if not years:
        st.warning("Không tìm thấy dữ liệu trong bucket.")
        return

    selected_year = st.selectbox("Chọn năm (folder)", years)
    file_choice = None
    if selected_year:
        year_files = files_by_year.get(selected_year, [])
        file_choice = st.selectbox("Chọn file CSV để phân tích / làm sạch", year_files)

    run_button = st.button("Chạy EDA & Làm sạch")

    z_threshold = 7.0
    null_threshold = 40

    if file_choice and run_button:
        st.subheader(f"File đang xử lý: {file_choice}")
        try:
            # Load dữ liệu
            df_orig = load_csv_from_minio(client, MINIO_BUCKET, file_choice)

            # Drop các cột cố định
            drop_cols = ['date_utc', 'latitude', 'longitude', '_ingested_at', 'year_utc']
            df_orig = df_orig.drop(columns=drop_cols, errors='ignore')
            st.markdown("### Các cột cố định đã bị loại bỏ:")
            st.write(drop_cols)
            st.write(f"Kích thước sau khi drop cột cố định: {df_orig.shape}")

            # Chạy EDA
            report, df_for_eda = run_eda(df_orig, z_threshold=z_threshold, null_threshold=null_threshold)

            st.markdown("### Các cột toàn rỗng")
            st.write(report.get('all_null_columns', []))

            st.markdown(f"### Cột bị drop theo ngưỡng null > {null_threshold}%")
            st.write(report.get('cols_dropped_by_null_threshold', []))

            st.markdown("### Các cột số phân tích")
            st.write(report.get('numeric_columns', []))

            st.markdown("### Tổng số hàng có ngoại lệ")
            st.write(report['outlier_analysis'].get('total_outlier_rows', 0))

            st.markdown("### Ngoại lệ theo cột")
            st.write(report['outlier_analysis'].get('outlier_counts_by_column', {}))

            st.markdown("### Mẫu dữ liệu ngoại lệ (20 hàng đầu)")
            st.write(pd.DataFrame(report['outlier_analysis'].get('outlier_samples', [])))

            st.markdown("### Mô tả thống kê")
            st.write(pd.DataFrame(report['describe']).T)

            # Lưu báo cáo EDA
            report_dir = "eda_reports"
            os.makedirs(report_dir, exist_ok=True)
            base_name = os.path.basename(file_choice).replace('.csv', '')
            report_path = os.path.join(report_dir, f"{base_name}_eda_report.xlsx")
            with st.spinner("Đang lưu báo cáo EDA..."):
                with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
                    pd.DataFrame(report['describe']).T.to_excel(writer, sheet_name='describe')
                    pd.DataFrame.from_dict(report['null_percent'], orient='index').to_excel(writer, sheet_name='null_percent')
            st.success(f"Đã lưu báo cáo: {report_path}")

            # Làm sạch dữ liệu và lưu MinIO
            st.markdown("### Đang làm sạch dữ liệu...")
            cleaned_df = run_cleaning(df_orig, report)
            st.markdown("### Dữ liệu sau khi làm sạch")
            st.dataframe(cleaned_df.head(20))

            if not client.bucket_exists(MINIO_CLEAN_BUCKET):
                client.make_bucket(MINIO_CLEAN_BUCKET)

            with st.spinner("Đang lưu file sạch lên MinIO..."):
                clean_csv = cleaned_df.to_csv(index=False).encode('utf-8')
                client.put_object(
                    MINIO_CLEAN_BUCKET,
                    file_choice,
                    data=BytesIO(clean_csv),
                    length=len(clean_csv),
                    content_type='text/csv'
                )
            st.success(f"Đã lưu file sạch lên bucket `{MINIO_CLEAN_BUCKET}`")

        except Exception as e:
            st.error(f"Lỗi khi xử lý file: {e}")