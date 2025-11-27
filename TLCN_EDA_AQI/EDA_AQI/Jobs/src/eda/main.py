import streamlit as st
import importlib
import sys
import os

# --- Đường dẫn tuyệt đối của các folder ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PREPROCESSING_DIR = os.path.join(CURRENT_DIR, "pre-processing")
EDA_DIR = os.path.join(CURRENT_DIR, "eda")
ML_DIR = os.path.join(CURRENT_DIR, "ML")  # Chứa train_model.py & predict_pm25.py

# Thêm vào sys.path để Python có thể import module
sys.path.extend([PREPROCESSING_DIR, EDA_DIR, ML_DIR])

# ==== Cấu hình trang ====
st.set_page_config(page_title="Hệ thống AQI & PM2.5", layout="wide")
st.sidebar.title("Chức năng")

# ==== Tiêu đề hệ thống ====
st.title("Hệ thống Phân tích thăm dò chất lượng không khí (AQI) và Mô hình dự đoán PM2.5")

# ==== MENU CHUNG ====
menu = st.sidebar.radio(
    "Chọn chức năng:",
    [
        "EDA & Làm sạch dữ liệu",
        "Trực quan hóa dữ liệu",
        "Phân tích nâng cao",
        "Huấn luyện mô hình PM2.5",
        "Dự đoán PM2.5"
    ]
)

# ==========================
# EDA & Làm sạch dữ liệu
# ==========================
if menu == "EDA & Làm sạch dữ liệu":
    st.subheader("EDA & Làm sạch dữ liệu")
    try:
        module = importlib.import_module("eda_cleaning")   # từ folder pre-processing
        module.run()
    except Exception as e:
        st.error(f"Lỗi khi tải module EDA & Làm sạch dữ liệu: {e}")

# ==========================
# Trực quan hóa dữ liệu
# ==========================
elif menu == "Trực quan hóa dữ liệu":
    st.subheader("Trực quan hóa dữ liệu")
    try:
        module = importlib.import_module("visualization")  # từ folder pre-processing
        module.run()
    except Exception as e:
        st.error(f"Lỗi khi tải module Trực quan hóa dữ liệu: {e}")

# ==========================
# Phân tích nâng cao
# ==========================
elif menu == "Phân tích nâng cao":
    st.subheader("Phân tích nâng cao")
    try:
        module = importlib.import_module("advanced_analysis")  # từ folder eda
        module.run()
    except Exception as e:
        st.error(f"Lỗi khi tải module Phân tích nâng cao: {e}")

# ==========================
# Huấn luyện mô hình PM2.5
# ==========================
elif menu == "Huấn luyện mô hình PM2.5":
    st.subheader("Huấn luyện mô hình PM2.5")
    try:
        train = importlib.import_module("train_model")  # từ folder ML
        train.main()
    except Exception as e:
        st.error(f"Lỗi khi tải module train_model: {e}")

# ==========================
# Dự đoán PM2.5
# ==========================
elif menu == "Dự đoán PM2.5":
    st.subheader("Dự đoán PM2.5")
    try:
        predict = importlib.import_module("predict_pm25")  # từ folder ML
        predict.main()
    except Exception as e:
        st.error(f"Lỗi khi tải module predict_pm25: {e}")