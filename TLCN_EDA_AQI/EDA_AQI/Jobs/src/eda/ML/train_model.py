# train_model_v7.py
import streamlit as st
import pandas as pd
from minio import Minio
from io import BytesIO
import joblib
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from lightgbm import LGBMRegressor
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_style("whitegrid")

# ==============================
# CẤU HÌNH MINIO
# ==============================
MINIO_HOST = "172.27.91.163:9004"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

BUCKET_DATA = "air-quality-clean"
PREFIX_DATA = "openmeteo/global/combined/"
BUCKET_EDA = "air-quality-eda"
CORR_OBJECT = "ma_tran_tuong_quan_latest.csv"
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
# MAIN APP
# ==============================
def main():
    st.title("Huấn luyện mô hình dự đoán PM2.5")

    # ---------- Load file dữ liệu ----------
    if "df_data" not in st.session_state:
        try:
            objects = client.list_objects(BUCKET_DATA, PREFIX_DATA, recursive=True)
            file_list = [obj.object_name for obj in objects if not obj.is_dir]
            if not file_list:
                st.warning("Không tìm thấy file nào trong bucket!")
                return
            st.session_state["file_list"] = file_list
        except Exception as e:
            st.error(f"Lỗi kết nối MinIO: {e}")
            return
    else:
        file_list = st.session_state["file_list"]

    file_select = st.selectbox("Chọn file dữ liệu sạch:", file_list)
    if not file_select:
        return

    if "df_data" not in st.session_state or st.session_state.get("current_file") != file_select:
        st.write(f"Đang tải dữ liệu từ file: {file_select}")
        try:
            data_obj = client.get_object(BUCKET_DATA, file_select)
            df = pd.read_csv(data_obj)
            st.session_state["df_data"] = df.copy()
            st.session_state["current_file"] = file_select
            st.success("Đã tải dữ liệu thành công!")
        except Exception as e:
            st.error(f"Không thể đọc file: {e}")
            return
    else:
        df = st.session_state["df_data"]
        st.info(f"Dữ liệu từ file `{file_select}` đã có trong session")
    
    st.dataframe(df.head())

    # ---------- Xử lý dữ liệu cơ bản ----------
    if "df_processed" not in st.session_state or st.session_state.get("processed_file") != file_select:
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors='coerce')
        df["year"] = df["ts_utc"].dt.year
        df["hour"] = df["ts_utc"].dt.hour
        df["day"] = df["ts_utc"].dt.day
        df["month"] = df["ts_utc"].dt.month

        le = LabelEncoder()
        df["location_encoded"] = le.fit_transform(df["location_key"])

        df_processed = df.drop(columns=["ts_utc", "location_key"])
        st.session_state["df_processed"] = df_processed
        st.session_state["le"] = le
        st.session_state["processed_file"] = file_select
    else:
        df_processed = st.session_state["df_processed"]
        le = st.session_state["le"]

    # ---------- Hiển thị ma trận tương quan ----------
    st.subheader("Ma trận tương quan")
    if "df_corr" not in st.session_state:
        try:
            corr_obj = client.get_object(BUCKET_EDA, CORR_OBJECT)
            df_corr = pd.read_csv(corr_obj, index_col=0)
            st.session_state["df_corr"] = df_corr
        except Exception as e:
            st.error(f"Không tìm thấy ma trận tương quan: {e}")
            return
    else:
        df_corr = st.session_state["df_corr"]

    st.dataframe(df_corr)

    # ---------- Chọn các đặc trưng ----------
    st.subheader("Chọn các đặc trưng quan trọng cho pm25")
    if "top_features" not in st.session_state:
        top_features = (
            df_corr["pm25"]
            .abs()
            .sort_values(ascending=False)
            .iloc[1:4]  # 3 đặc trưng cao nhất sau pm25
            .index.tolist()
        )
        st.session_state["top_features"] = top_features
    else:
        top_features = st.session_state["top_features"]

    st.markdown("**Cách chọn:** chọn 3 cột có tương quan tuyệt đối cao nhất với `pm25` (trừ chính `pm25`).")
    st.write("Các đặc trưng được chọn:", top_features)

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

        # Lưu vào session
        st.session_state["model"] = model
        st.session_state["X_train"] = X_train
        st.session_state["X_test"] = X_test
        st.session_state["y_train"] = y_train
        st.session_state["y_test"] = y_test

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