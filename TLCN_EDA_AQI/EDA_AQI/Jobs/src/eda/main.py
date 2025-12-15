import streamlit as st
import importlib
import sys
import os

# =========================
# 0) Paths (absolute)
# =========================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

PREPROCESSING_DIR = os.path.join(CURRENT_DIR, "pre-processing")
EDA_DIR          = os.path.join(CURRENT_DIR, "eda")
ML_DIR           = os.path.join(CURRENT_DIR, "ML")

# Add to sys.path so we can import modules by file name
for p in [CURRENT_DIR, PREPROCESSING_DIR, EDA_DIR, ML_DIR]:
    if p not in sys.path:
        sys.path.append(p)

# =========================
# 1) Streamlit config
# =========================
st.set_page_config(page_title="Hệ thống AQI & PM2.5", layout="wide")

st.sidebar.title("Chức năng")

st.title("Hệ thống Phân tích thăm dò chất lượng không khí (AQI) và Mô hình dự đoán PM2.5")

menu = st.sidebar.radio(
    "Chọn chức năng:",
    [
        "EDA & Làm sạch dữ liệu",
        "Trực quan hóa dữ liệu",
        "Phân tích nâng cao (EDA nâng cao)",
        "Huấn luyện mô hình PM2.5",
        "Dự đoán PM2.5",
    ],
)

# =========================
# 2) Helper: load & run
# =========================
def load_and_run(module_name: str, fn_name: str = "run"):
    """
    Import module by name and call function inside it.
    Default function is run(). For ML modules, use main().
    """
    try:
        module = importlib.import_module(module_name)

        if not hasattr(module, fn_name):
            st.error(f"Module '{module_name}' không có hàm '{fn_name}()'.")
            return

        getattr(module, fn_name)()

    except ModuleNotFoundError as e:
        st.error(f"Không tìm thấy module: {module_name}. Chi tiết: {e}")
    except Exception as e:
        st.error(f"Lỗi khi chạy module '{module_name}': {e}")

# =========================
# 3) Routing
# =========================
if menu == "EDA & Làm sạch dữ liệu":
    st.subheader("EDA & Làm sạch dữ liệu")
    # file: pre-processing/eda_cleaning.py
    load_and_run("eda_cleaning", "run")

elif menu == "Trực quan hóa dữ liệu":
    st.subheader("Trực quan hóa dữ liệu")
    # file: pre-processing/visualization.py
    load_and_run("visualization", "run")

elif menu == "Phân tích nâng cao (EDA nâng cao)":
    st.subheader("Phân tích nâng cao (EDA nâng cao)")
    # file: eda/advanced_analysis.py
    load_and_run("advanced_analysis", "run")

elif menu == "Huấn luyện mô hình PM2.5":
    st.subheader("Huấn luyện mô hình PM2.5")
    # file: ML/train_model.py
    load_and_run("train_model", "main")

elif menu == "Dự đoán PM2.5":
    st.subheader("Dự đoán PM2.5")
    # file: ML/predict_pm25.py
    load_and_run("predict_pm25", "main")
