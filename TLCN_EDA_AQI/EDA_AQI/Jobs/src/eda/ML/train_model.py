# train_model_v8_no_heatmap.py
import streamlit as st
import pandas as pd
from minio import Minio
from io import BytesIO
import joblib
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from lightgbm import LGBMRegressor
import seaborn as sns
import re

sns.set_style("whitegrid")

# ==============================
# CẤU HÌNH MINIO
# ==============================
MINIO_HOST = "172.27.91.163:9004"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

BUCKET_DATA = "air-quality-clean"
PREFIX_DATA = "openmeteo/global/"
BUCKET_EDA = "air-quality-eda"
BUCKET_MODEL = "air-quality-model"

# ==============================
# KẾT NỐI MINIO
# ==============================
client = Minio(
    endpoint=MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# ==============================
# HELPER: LƯU MODEL
# ==============================
def save_model_to_minio(model, features, le, start_year, end_year):
    file_name = f"lightgbm_pm25_model_{start_year}_{end_year}.pkl"
    buffer = BytesIO()
    joblib.dump({
        "model": model,
        "features": features,
        "label_encoder": le
    }, buffer)
    buffer.seek(0)

    if not client.bucket_exists(BUCKET_MODEL):
        client.make_bucket(BUCKET_MODEL)

    client.put_object(
        BUCKET_MODEL,
        file_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    return file_name

# ==============================
# HELPER: Đọc CSV từ MinIO với cache
# ==============================
@st.cache_data(show_spinner=False)
def load_csv_from_minio(bucket, object_name, index_col=None):
    obj = client.get_object(bucket, object_name)
    return pd.read_csv(obj, index_col=index_col)

# ==============================
# MAIN APP
# ==============================
def main():
    st.title("Huấn luyện mô hình dự đoán PM2.5")

    # ---------- Load file dữ liệu sạch ----------
    st.subheader("Chọn file dữ liệu sạch")
    try:
        all_objects = client.list_objects(BUCKET_DATA, prefix=PREFIX_DATA, recursive=True)
        file_list = [obj.object_name for obj in all_objects if obj.object_name.endswith(".csv")]
        if not file_list:
            st.warning("Không tìm thấy file CSV nào trong bucket dữ liệu sạch!")
            return
    except Exception as e:
        st.error(f"Lỗi kết nối MinIO: {e}")
        return

    file_select = st.selectbox("Chọn file dữ liệu sạch:", file_list)
    if not file_select:
        return

    # Load dữ liệu
    try:
        df = load_csv_from_minio(BUCKET_DATA, file_select)
        st.success(f"Đã tải dữ liệu thành công từ file `{file_select}`")
    except Exception as e:
        st.error(f"Không thể đọc file: {e}")
        return

    st.dataframe(df.head())

    # ---------- Xử lý dữ liệu cơ bản ----------
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors='coerce')
    df["year"] = df["ts_utc"].dt.year
    df["hour"] = df["ts_utc"].dt.hour
    df["day"] = df["ts_utc"].dt.day
    df["month"] = df["ts_utc"].dt.month

    le = LabelEncoder()
    df["location_encoded"] = le.fit_transform(df["location_key"])
    df_processed = df.drop(columns=["ts_utc", "location_key"])

    # ---------- Chọn năm và file ma trận tương quan ----------
    st.subheader("Chọn năm và file ma trận tương quan")
    try:
        all_objects = client.list_objects(BUCKET_EDA, recursive=True)
        folder_set = set()
        for obj in all_objects:
            m = re.match(r"(tuong_quan_\d{4})/", obj.object_name)
            if m:
                folder_set.add(m.group(1))
        folder_list = sorted(list(folder_set))
        if not folder_list:
            st.warning("Không tìm thấy folder tuong_quan_<năm> trong bucket EDA!")
            return
    except Exception as e:
        st.error(f"Lỗi kết nối MinIO: {e}")
        return

    year_select = st.selectbox("Chọn năm tương quan:", folder_list)

    # List file CSV trong folder đã chọn
    try:
        all_objects_corr = client.list_objects(BUCKET_EDA, prefix=f"{year_select}/", recursive=True)
        corr_files = [obj.object_name for obj in all_objects_corr if obj.object_name.endswith(".csv")]
        if not corr_files:
            st.warning(f"Không tìm thấy file CSV trong folder {year_select}!")
            return
    except Exception as e:
        st.error(f"Lỗi khi lấy file CSV: {e}")
        return

    corr_file_select = st.selectbox("Chọn file ma trận tương quan:", corr_files)

    # Load ma trận tương quan
    try:
        df_corr = load_csv_from_minio(BUCKET_EDA, corr_file_select, index_col=0)
        # Chuyển tất cả về float (nếu lỗi -> object)
        df_corr = df_corr.apply(pd.to_numeric, errors='coerce')
        df_corr.columns = df_corr.columns.astype(str)
        df_corr.index = df_corr.index.astype(str)
        st.success(f"Đã tải ma trận tương quan từ file `{corr_file_select}`")

        # Hiển thị ma trận tương quan
        st.subheader("Ma trận tương quan")
        st.dataframe(df_corr)
    except Exception as e:
        st.error(f"Lỗi khi đọc ma trận tương quan: {e}")
        return

    # ---------- Chọn các đặc trưng ----------
    st.subheader("Chọn các đặc trưng quan trọng cho pm25")
    try:
        top_features = (
            df_corr["pm25"]
            .abs()
            .sort_values(ascending=False)
            .drop("pm25")  # loại bỏ chính nó
            .head(3)
            .index
            .tolist()
        )
        st.write("Các đặc trưng được chọn:", top_features)
    except Exception as e:
        st.error(f"Lỗi khi chọn đặc trưng: {e}")
        return

    X = df_processed[top_features]
    y = df_processed["pm25"]

    # ---------- Huấn luyện model ----------
    if st.button("Bắt đầu huấn luyện"):
        st.info("Đang train mô hình LightGBM...")

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        st.write(f"Số dòng train: {len(X_train)}, test: {len(X_test)}")

        model = LGBMRegressor(
            n_estimators=500,
            learning_rate=0.05,
            max_depth=-1,
            num_leaves=128,
            random_state=42,
            verbose=-1
        )
        model.fit(X_train, y_train)

        st.success("Huấn luyện xong!")
        score = model.score(X_test, y_test)
        st.success(f"R² trên tập test: {score:.4f}")

        # Lưu model lên MinIO
        start_year = df_processed["year"].min()
        end_year = df_processed["year"].max()
        file_name = save_model_to_minio(model, top_features, le, start_year, end_year)
        st.success(f"Đã lưu mô hình: `{file_name}` vào MinIO")

if __name__ == "__main__":
    main()