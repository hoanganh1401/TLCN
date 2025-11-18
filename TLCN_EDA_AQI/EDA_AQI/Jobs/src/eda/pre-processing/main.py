import streamlit as st
import importlib
import sys
import os

# --- Thêm đường dẫn tuyệt đối của thư mục chứa main.py ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_DIR)

st.set_page_config(page_title="AQI Dashboard", layout="wide")

st.sidebar.title("Chức năng")

page = st.sidebar.radio(
    "Chọn tác vụ:",
    [
        "EDA & Làm sạch dữ liệu",
        "Trực quan hóa dữ liệu"
    ]
)

st.title("Hệ thống EDA & Visualization AQI")

# ======================================
# ĐÚNG TÊN MODULE CỦA BẠN
# ======================================
if page == "EDA & Làm sạch dữ liệu":
    module = importlib.import_module("eda_cleaning")   # <── đúng tên file
    module.run()  # đảm bảo trong file có hàm run()

elif page == "Trực quan hóa dữ liệu":
    module = importlib.import_module("visualization")  # <── đúng tên file
    module.run()
