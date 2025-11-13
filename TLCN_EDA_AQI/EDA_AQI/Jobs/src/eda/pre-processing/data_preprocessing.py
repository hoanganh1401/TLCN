import streamlit as st
from minio import Minio
from io import BytesIO
import pandas as pd
import numpy as np
from scipy import stats
from sklearn.preprocessing import MinMaxScaler
import time

# =============================
# CẤU HÌNH MINIO
# =============================
MINIO_HOST = "172.27.91.163:9004"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "air-quality"
MINIO_CLEAN_BUCKET = "air-quality-clean"

# =============================
# CẤU HÌNH THAM SỐ XỬ LÝ
# =============================
Z_THRESHOLD = 7.0          # Ngưỡng Z-Score mặc định
NULL_THRESHOLD = 40.0      # Ngưỡng % null để loại cột

# =============================
# KẾT NỐI MINIO
# =============================
client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

st.set_page_config(page_title="Làm sạch dữ liệu AQI", layout="wide")
st.title("🌍 Làm sạch dữ liệu chất lượng không khí (theo năm)")

# Kiểm tra kết nối
try:
    if not client.bucket_exists(MINIO_BUCKET):
        st.error(f"❌ Bucket '{MINIO_BUCKET}' không tồn tại!")
        st.stop()
    else:
        st.success(f"✅ Kết nối thành công đến bucket '{MINIO_BUCKET}'")
except Exception as e:
    st.error(f"Lỗi kết nối: {e}")
    st.stop()

# =============================
# LẤY DANH SÁCH NĂM TRONG THƯ MỤC openmeteo/global/
# =============================
try:
    all_objects = list(client.list_objects(MINIO_BUCKET, recursive=True))
    paths = [obj.object_name for obj in all_objects]

    # Lấy danh sách năm trong thư mục openmeteo/global/
    years = sorted({
        p.split("/")[2] for p in paths
        if p.startswith("openmeteo/global/") and len(p.split("/")) >= 3
    })

    if not years:
        st.warning("⚠️ Không tìm thấy thư mục năm trong 'openmeteo/global/'.")
        st.stop()

    selected_year = st.selectbox("📅 Chọn năm cần xử lý:", years)
    if not selected_year:
        st.stop()
except Exception as e:
    st.error(f"Lỗi khi lấy danh sách năm: {e}")
    st.stop()

# =============================
# BẮT ĐẦU XỬ LÝ DỮ LIỆU
# =============================
if st.button("🚀 Bắt đầu làm sạch dữ liệu"):
    with st.spinner(f"Đang xử lý dữ liệu năm {selected_year}..."):
        # Lọc các file CSV thuộc năm được chọn
        year_files = [
            p for p in paths
            if p.startswith(f"openmeteo/global/{selected_year}/") and p.endswith(".csv")
        ]

        if not year_files:
            st.warning(f"⚠️ Không tìm thấy file CSV nào trong năm {selected_year}.")
            st.stop()

        progress = st.progress(0)
        total_files = len(year_files)

        for i, file_path in enumerate(year_files):
            st.subheader(f"📂 File: `{file_path}`")

            try:
                # Đọc file CSV từ MinIO
                response = client.get_object(MINIO_BUCKET, file_path)
                data = response.read()
                response.close()
                response.release_conn()
                df = pd.read_csv(BytesIO(data))
                st.write("**📘 Dữ liệu gốc:**")
                st.dataframe(df.head())

                # ===============================
                # BƯỚC 0: XOÁ CỘT KHÔNG CẦN THIẾT
                # ===============================
                drop_cols = ['date_utc', '_ingested_at']
                existing_drop_cols = [col for col in drop_cols if col in df.columns]
                if existing_drop_cols:
                    df = df.drop(columns=existing_drop_cols)
                    st.info(f"🗑️ Đã xoá các cột không cần thiết: {existing_drop_cols}")
                else:
                    st.success("✅ Không có cột 'date_utc' hoặc '_ingested_at' trong dữ liệu.")

                # ===============================
                # BƯỚC 1: XOÁ CỘT TOÀN NULL
                # ===============================
                all_null_cols = df.columns[df.isnull().all()]
                df = df.dropna(axis=1, how="all")
                if len(all_null_cols) > 0:
                    st.warning(f"⚠️ Đã xoá cột toàn null: {list(all_null_cols)}")
                else:
                    st.success("✅ Không có cột nào toàn null.")

                # ===============================
                # BƯỚC 2: XOÁ CỘT NULL > 40%
                # ===============================
                null_percent = df.isnull().mean() * 100
                cols_to_drop = null_percent[null_percent > NULL_THRESHOLD].index
                if len(cols_to_drop) > 0:
                    st.warning(f"⚠️ Cột bị loại vì >{NULL_THRESHOLD}% null: {list(cols_to_drop)}")
                    df = df.drop(columns=cols_to_drop)
                else:
                    st.success("✅ Không có cột nào vượt ngưỡng null 40%.")
                st.write("**📊 Sau khi xử lý giá trị null:**")
                st.dataframe(df.head())

                # ===============================
                # BƯỚC 3: XOÁ DÒNG CÓ GIÁ TRỊ RỖNG
                # ===============================
                before_rows = len(df)
                df = df.dropna(how="any")
                st.info(f"🧹 Đã xoá {before_rows - len(df)} dòng chứa giá trị null còn sót lại.")

                # ===============================
                # BƯỚC 4: LOẠI NGOẠI LỆ (Z-SCORE)
                # ===============================
                numeric_df = df.select_dtypes(include=[np.number])
                valid_cols = numeric_df.loc[:, numeric_df.std() > 0]

                if not valid_cols.empty:
                    z = np.abs(stats.zscore(valid_cols, nan_policy='omit'))
                    filtered = (z < Z_THRESHOLD).all(axis=1)
                    removed = len(df) - filtered.sum()
                    df = df[filtered]
                    st.info(f"⚙️ Đã loại {removed} dòng ngoại lệ theo Z-Score > {Z_THRESHOLD}.")
                else:
                    st.warning("⚠️ Không có cột số hợp lệ để tính Z-score.")

                st.write("**📈 Sau khi loại ngoại lệ:**")
                st.dataframe(df.head())

                # ===============================
                # BƯỚC 5: CHUẨN HÓA (bỏ qua hoặc bật lại nếu cần)
                # ===============================
                # numeric_cols = df.select_dtypes(include=[np.number]).columns
                # if len(df) > 0 and len(numeric_cols) > 0:
                #     scaler = MinMaxScaler()
                #     df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
                #     st.success("🔧 Đã chuẩn hóa dữ liệu (MinMaxScaler 0-1).")
                # else:
                #     st.warning("Không có cột số để chuẩn hóa.")
                # st.write("**📊 Dữ liệu sau khi chuẩn hóa:**")
                # st.dataframe(df.head())

                # ===============================
                # BƯỚC 6: LƯU FILE SẠCH LÊN MINIO
                # ===============================
                clean_path = file_path  # Giữ nguyên cấu trúc đường dẫn
                if not client.bucket_exists(MINIO_CLEAN_BUCKET):
                    client.make_bucket(MINIO_CLEAN_BUCKET)

                clean_csv = df.to_csv(index=False).encode("utf-8")
                client.put_object(
                    MINIO_CLEAN_BUCKET,
                    clean_path,
                    data=BytesIO(clean_csv),
                    length=len(clean_csv),
                    content_type="text/csv",
                )

                st.success(f"💾 Đã lưu file sạch: `{clean_path}`")

            except Exception as e:
                st.error(f"❌ Lỗi khi xử lý file {file_path}: {e}")

            progress.progress((i + 1) / total_files)
            time.sleep(0.2)

        st.success(f"🎉 Hoàn tất xử lý dữ liệu năm {selected_year}!")
