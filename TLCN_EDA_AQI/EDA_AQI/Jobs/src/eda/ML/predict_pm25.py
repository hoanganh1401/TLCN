import streamlit as st
import pandas as pd
import numpy as np
from minio import Minio
from io import BytesIO
import joblib

# ==========================
# CONFIG
# ==========================
MINIO_HOST = "172.27.91.163:9004"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"

BUCKET_MODEL = "air-quality-model"
BUCKET_INPUT = "air-quality-data-test"
BUCKET_RESULT = "air-quality-prediction"

# ==========================
# MINIO CLIENT
# ==========================
@st.cache_resource
def get_minio_client():
    return Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

client = get_minio_client()

# ==========================
# HÀM HỖ TRỢ
# ==========================
def read_csv_from_minio(bucket, object_name):
    obj = client.get_object(bucket, object_name)
    df = pd.read_csv(BytesIO(obj.read()))
    return df

def save_result_to_minio(df, start_date, end_date):
    df_to_save = df[["Ngày", "PM2.5"]]
    object_name = f"pm25_prediction_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"

    csv_buffer = BytesIO()
    df_to_save.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    csv_buffer.seek(0)

    if not client.bucket_exists(BUCKET_RESULT):
        client.make_bucket(BUCKET_RESULT)

    client.put_object(
        BUCKET_RESULT,
        object_name,
        csv_buffer,
        length=csv_buffer.getbuffer().nbytes
    )
    st.success(f"Kết quả đã lưu MinIO: {BUCKET_RESULT}/{object_name}")
    return object_name

def safe_prepare_data(df, features, le):
    df = df.copy()

    if "ts_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors="coerce")
        df["hour"] = df["ts_utc"].dt.hour.fillna(0).astype(int)
        df["day"] = df["ts_utc"].dt.day.fillna(1).astype(int)
        df["month"] = df["ts_utc"].dt.month.fillna(1).astype(int)

    df.drop("pm25", axis=1, inplace=True, errors="ignore")

    numeric_cols = [
        'hour', 'day', 'month', 'pm10', 'no2', 'o3', 'so2', 'co',
        'aod', 'dust', 'uv_index', 'aqi', 'aqi_pm25', 'aqi_pm10',
        'aqi_no2', 'aqi_o3', 'aqi_so2', 'aqi_co'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "location_key" in df.columns:
        unseen = set(df["location_key"].dropna()) - set(le.classes_)
        if unseen:
            st.warning(f"Phát hiện location chưa train: {', '.join(unseen)} — sẽ tự động loại bỏ.")
            df = df[df["location_key"].isin(le.classes_)]
        if df.empty:
            st.error("Không còn dòng hợp lệ sau khi lọc location.")
            st.stop()
        df["location_encoded"] = le.transform(df["location_key"])
        df.drop("location_key", axis=1, inplace=True)

    df = df.fillna(0)

    missing = [f for f in features if f not in df.columns]
    for f in missing:
        df[f] = 0

    X = df[features].astype(float)
    st.success(f"Đã xử lý {len(X)} dòng dữ liệu hợp lệ!")
    return X

# ==========================
# LOAD MODEL DYNAMIC
# ==========================
@st.cache_resource(show_spinner="Đang tải mô hình...")
def load_model_from_minio(object_name):
    obj = client.get_object(BUCKET_MODEL, object_name)
    data = joblib.load(BytesIO(obj.read()))
    return data["model"], data["features"], data["label_encoder"]

# ==========================
# MAIN APP
# ==========================
def main():
    st.title("Dự đoán PM2.5 từ MinIO")

    # --- Chọn file mô hình ---
    if not client.bucket_exists(BUCKET_MODEL):
        st.warning(f"Bucket {BUCKET_MODEL} chưa tồn tại.")
        st.stop()

    model_objects = client.list_objects(BUCKET_MODEL)
    model_files = [obj.object_name for obj in model_objects if obj.object_name.endswith(".pkl")]
    selected_model_file = st.selectbox("Chọn mô hình từ MinIO", model_files)

    if not selected_model_file:
        st.warning("Chưa có file mô hình nào trong MinIO")
        st.stop()

    model, features, le = load_model_from_minio(selected_model_file)
    st.success(f"Đã tải mô hình: {selected_model_file}")

    # --- Chọn file dữ liệu ---
    if not client.bucket_exists(BUCKET_INPUT):
        st.warning(f"Bucket {BUCKET_INPUT} chưa tồn tại.")
        st.stop()

    objects = client.list_objects(BUCKET_INPUT)
    files = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]
    selected_file = st.selectbox("Chọn file dữ liệu từ MinIO", files)

    if not selected_file:
        st.warning("Chưa có file dữ liệu nào trong MinIO")
        st.stop()

    df_input = read_csv_from_minio(BUCKET_INPUT, selected_file)

    if "Ngày" not in df_input.columns and "ts_utc" in df_input.columns:
        df_input.rename(columns={"ts_utc": "Ngày"}, inplace=True)

    st.dataframe(df_input, width='stretch')

    # Dự đoán 7 ngày tiếp theo
    last_date = pd.to_datetime(df_input["Ngày"]).max()
    X = safe_prepare_data(df_input.rename(columns={"Ngày": "ts_utc"}), features, le)

    if st.button("DỰ ĐOÁN 7 NGÀY TIẾP THEO"):
        with st.spinner("Predicting..."):
            preds = np.clip(model.predict(X), 0, None)
            preds = np.round(preds, 1)

            future_days = [(last_date + pd.Timedelta(days=i+1)).date() for i in range(7)]
            if len(preds) < 7:
                last_pred = preds[-1]
                preds = np.concatenate([preds, np.full(7 - len(preds), last_pred)])

            result = pd.DataFrame({
                "Ngày": future_days,
                "PM2.5": preds[:7]
            })

            start_date = future_days[0]
            end_date = future_days[-1]

            # Lưu kết quả
            save_result_to_minio(result, start_date, end_date)

        st.success("Dự đoán hoàn tất và đã lưu vào MinIO.")
        st.dataframe(result, width='stretch')

if __name__ == "__main__":
    main()