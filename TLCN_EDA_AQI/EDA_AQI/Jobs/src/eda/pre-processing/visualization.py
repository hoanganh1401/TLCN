import streamlit as st
from minio import Minio
from io import BytesIO
import pandas as pd
import matplotlib.pyplot as plt

# ==============================
# CẤU HÌNH MINIO
# ==============================
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
    resp = client.get_object(bucket, path)
    data = resp.read()
    resp.close()
    resp.release_conn()
    return pd.read_csv(BytesIO(data))

# ==============================
# GIAO DIỆN CHÍNH
# ==============================
def run():
    st.set_page_config(page_title="So sánh dữ liệu sạch", layout="wide")
    st.title("Phân tích dữ liệu sạch")

    # ========== SESSION STATE ==========
    if "mode" not in st.session_state:
        st.session_state.mode = None
    if "loaded_dfs" not in st.session_state:
        st.session_state.loaded_dfs = {}

    # ========== KẾT NỐI MINIO ==========
    client = get_minio_client()
    try:
        if not client.bucket_exists(MINIO_CLEAN_BUCKET):
            st.error(f"Bucket '{MINIO_CLEAN_BUCKET}' không tồn tại!")
            st.stop()
        else:
            st.success(f"Kết nối MinIO ({MINIO_CLEAN_BUCKET}) thành công")
    except Exception as e:
        st.error(f"Lỗi kết nối MinIO: {e}")
        st.stop()

    years, files_by_year = list_clean_files_by_year(client, MINIO_CLEAN_BUCKET)

    if not years:
        st.warning("Không có dữ liệu để hiển thị.")
        st.stop()

    # ==============================
    # 2 NÚT CHỌN CHẾ ĐỘ (luôn xuất hiện)
    # ==============================
    col1, col2 = st.columns(2)

    with col1:
        if st.button("Vẽ biểu đồ theo năm"):
            st.session_state.mode = "plot"

    with col2:
        if st.button("So sánh nhiều năm"):
            st.session_state.mode = "compare"

    mode = st.session_state.mode

    # Nếu chưa chọn gì thì dừng
    if mode is None:
        st.info("Hãy chọn chế độ để bắt đầu.")
        st.stop()

    # ==============================
    # CHẾ ĐỘ 1: VẼ BIỂU ĐỒ 1 NĂM
    # ==============================
    if mode == "plot":
        st.subheader("Chọn 1 năm")

        year = st.selectbox("Chọn năm", years)

        file_path = files_by_year[year][0]

        if file_path not in st.session_state.loaded_dfs:
            df = load_clean_csv(client, MINIO_CLEAN_BUCKET, file_path)
            st.session_state.loaded_dfs[file_path] = df
        else:
            df = st.session_state.loaded_dfs[file_path]

        # Vẽ histogram
        important_cols = ["aqi", "pm25", "pm10", "co"]
        numeric_cols = [c for c in important_cols if c in df.columns]

        st.markdown(f"## Histogram cho năm {year}")

        for col in numeric_cols:
            fig, ax = plt.subplots(figsize=(7, 4))
            ax.hist(df[col].dropna(), bins=20, alpha=0.7)
            ax.set_title(f"{col} - {year}")
            ax.grid(True, linestyle="--", alpha=0.4)
            st.pyplot(fig)

    # ==============================
    # CHẾ ĐỘ 2: SO SÁNH NHIỀU NĂM
    # ==============================
    if mode == "compare":
        st.subheader("Chọn nhiều năm để so sánh")

        selected_years = st.multiselect("Chọn năm", years)

        if len(selected_years) < 2:
            st.warning("Chọn ít nhất 2 năm để so sánh.")
            st.stop()

        dfs = []
        for year in selected_years:
            file_path = files_by_year[year][0]

            if file_path not in st.session_state.loaded_dfs:
                df = load_clean_csv(client, MINIO_CLEAN_BUCKET, file_path)
                st.session_state.loaded_dfs[file_path] = df
            else:
                df = st.session_state.loaded_dfs[file_path]

            df["year"] = year
            dfs.append(df)

        df_all = pd.concat(dfs, ignore_index=True)

        important_cols = ["aqi", "pm25", "pm10", "co"]
        numeric_cols = [c for c in important_cols if c in df_all.columns]

        st.markdown("## So sánh các năm")

        for col in numeric_cols:
            fig, ax = plt.subplots(figsize=(8, 5))

            for year in selected_years:
                ax.hist(df_all[df_all["year"] == year][col].dropna(), bins=20, alpha=0.5, label=year)

            ax.set_title(f"So sánh {col}")
            ax.legend()
            ax.grid(True, linestyle="--", alpha=0.4)

            st.pyplot(fig)
