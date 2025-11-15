# visualization.py
import streamlit as st
from minio import Minio
from io import BytesIO
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# ==============================
# CẤU HÌNH MINIO
# ==============================
#MINIO_HOST = "localhost:9004"
MINIO_HOST = "172.27.91.163:9004"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_CLEAN_BUCKET = "air-quality-clean"

# ==============================
# HÀM TIỆN ÍCH
# ==============================
def get_minio_client():
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def list_clean_files_by_year(client, bucket):
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

def load_clean_csv(client, bucket, path):
    response = client.get_object(bucket, path)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_csv(BytesIO(data))

# ==============================
# HÀM RUN CHÍNH
# ==============================
def run():
    st.set_page_config(page_title="Trực quan hóa dữ liệu sạch", layout="wide")
    st.title("Trực quan hóa dữ liệu sạch từ MinIO")

    # ==============================
    # Khởi tạo session_state
    # ==============================
    if "loaded_dfs" not in st.session_state:
        st.session_state.loaded_dfs = {}  # key = file path, value = df
    if "current_file" not in st.session_state:
        st.session_state.current_file = None

    client = get_minio_client()

    # Kiểm tra bucket
    try:
        if not client.bucket_exists(MINIO_CLEAN_BUCKET):
            st.error(f"Bucket '{MINIO_CLEAN_BUCKET}' không tồn tại! Hãy chạy làm sạch trước.")
            st.stop()
        else:
            st.success(f"Kết nối MinIO ({MINIO_CLEAN_BUCKET}) thành công")
    except Exception as e:
        st.error(f"Lỗi kết nối MinIO: {e}")
        st.stop()

    # Lấy danh sách file theo năm
    years, files_by_year = list_clean_files_by_year(client, MINIO_CLEAN_BUCKET)
    if not years:
        st.warning("Không tìm thấy dữ liệu trong bucket.")
        st.stop()

    # ==============================
    # Chọn năm & file
    # ==============================
    selected_year = st.selectbox("Chọn năm dữ liệu sạch", years)
    file_choice = None
    if selected_year:
        year_files = files_by_year.get(selected_year, [])
        if not year_files:
            st.warning(f"Không có file CSV nào trong năm {selected_year}")
            st.stop()
        file_choice = st.selectbox("Chọn file CSV để vẽ", year_files)

    # Nút chạy
    run_button = st.button("Vẽ biểu đồ")

    # ==============================
    # Load DataFrame (từ session_state nếu đã load trước đó)
    # ==============================
    df = None
    if file_choice:
        if (file_choice not in st.session_state.loaded_dfs) or (st.session_state.current_file != file_choice):
            df = load_clean_csv(client, MINIO_CLEAN_BUCKET, file_choice)
            st.session_state.loaded_dfs[file_choice] = df
            st.session_state.current_file = file_choice
        else:
            df = st.session_state.loaded_dfs[file_choice]

    # ==============================
    # Vẽ biểu đồ nếu có df
    # ==============================
    if df is not None and (run_button or st.session_state.current_file == file_choice):

        # Chuyển cột thời gian
        if "ts_utc" in df.columns:
            df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors="coerce")
            df = df.sort_values("ts_utc")
            st.success("Đã chuyển đổi ts_utc sang datetime!")
        else:
            st.warning("File không có cột ts_utc — không thể vẽ line chart.")

        st.markdown("## Thông tin dữ liệu")
        st.write(df.head())

        # Các cột quan trọng
        important_cols = ["aqi", "pm25", "pm10", "co"]
        numeric_cols_important = [c for c in important_cols if c in df.columns]
        if not numeric_cols_important:
            st.warning("Không tìm thấy cột quan trọng trong file: aqi, pm25, pm10, co")

        # ==============================
        # Histogram
        # ==============================
        if numeric_cols_important:
            st.markdown("## Histogram các chỉ số chính")
            for col in numeric_cols_important:
                fig, ax = plt.subplots(figsize=(6, 4))
                counts, bins, patches = ax.hist(df[col].dropna(), bins=20, color='skyblue', edgecolor='black')
                for count, patch in zip(counts, patches):
                    if count > 0:
                        ax.text(patch.get_x() + patch.get_width()/2, count, f"{int(count)}", ha='center', va='bottom', fontsize=9)
                ax.set_title(f"Histogram: {col}")
                ax.set_xlabel(col)
                ax.set_ylabel("Frequency")
                ax.grid(True, linestyle='--', alpha=0.5)
                st.pyplot(fig)