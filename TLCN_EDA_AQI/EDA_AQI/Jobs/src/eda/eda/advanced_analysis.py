import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sklearn.feature_selection import SelectKBest, f_regression, mutual_info_regression
from minio import Minio
from io import BytesIO
import warnings
warnings.filterwarnings('ignore')
import json

# =============================
# CẤU HÌNH MINIO
# =============================
MINIO_HOST = "172.27.91.163:9004"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_CLEAN_BUCKET = "air-quality-clean"

# Cấu hình trang
st.set_page_config(
    page_title="Phân Tích Chất Lượng Không Khí Chuyên Sâu",
    layout="wide"
)

# CSS cho styling
st.markdown("""
<style>
.metric-card {
    background-color: #f0f2f6;
    border-radius: 10px;
    padding: 20px;
    margin: 10px 0;
}
.pollution-high {
    color: #ff4444;
    font-weight: bold;
}
.pollution-moderate {
    color: #ffaa44;
    font-weight: bold;
}
.pollution-good {
    color: #44aa44;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)

def get_minio_client():
    """Tạo MinIO client"""
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def list_combined_files(client, bucket):
    """Liệt kê các file CSV trong thư mục openmeteo/global"""
    try:
        all_objects = list(client.list_objects(bucket, prefix="openmeteo/global/", recursive=True))
        csv_files = []
        for obj in all_objects:
            # Lấy tất cả file CSV, không chỉ combined
            if obj.object_name.endswith('.csv'):
                csv_files.append(obj.object_name)
        return sorted(csv_files)  # Sắp xếp để dễ chọn
    except Exception as e:
        st.error(f"Lỗi khi liệt kê files: {e}")
        return []

def load_csv_from_minio(client, bucket, path, max_retries=3):
    """Load CSV từ MinIO với retry logic và chunk reading cho file lớn"""
    import time
    
    for attempt in range(max_retries):
        try:
            st.info(f"Đang tải file (lần thử {attempt + 1}/{max_retries})...")
            
            # Đọc file theo chunks để tránh timeout
            response = client.get_object(bucket, path)
            
            # Đọc dữ liệu theo chunks
            chunk_size = 8192  # 8KB mỗi chunk
            chunks = []
            total_size = 0
            
            # Hiển thị progress
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
                total_size += len(chunk)
                
                # Cập nhật progress (ước tính)
                if total_size % (chunk_size * 100) == 0:  # Cập nhật mỗi 800KB
                    status_text.text(f"Đã tải: {total_size / (1024*1024):.1f} MB")
            
            progress_bar.progress(100)
            status_text.text(f"Hoàn tất: {total_size / (1024*1024):.1f} MB")
            
            # Kết hợp chunks và đọc CSV
            data = b''.join(chunks)
            response.close()
            response.release_conn()
            
            st.success(f" Đã tải thành công {total_size / (1024*1024):.1f} MB")
            return pd.read_csv(BytesIO(data))
            
        except Exception as e:
            if attempt < max_retries - 1:
                st.warning(f"Lỗi lần {attempt + 1}: {str(e)[:100]}... Thử lại sau 2 giây...")
                time.sleep(2)
            else:
                st.error(f" Lỗi sau {max_retries} lần thử: {e}")
                return None
    
    return None

@st.cache_data
def load_data(selected_file):
    """Load dữ liệu từ MinIO với file được chọn"""
    try:
        client = get_minio_client()
        
        # Kiểm tra bucket tồn tại
        if not client.bucket_exists(MINIO_CLEAN_BUCKET):
            st.error(f"Bucket '{MINIO_CLEAN_BUCKET}' không tồn tại!")
            st.info("Hãy chạy quy trình làm sạch dữ liệu trước.")
            return None
        
        st.success(f"Kết nối MinIO thành công: {MINIO_HOST}")
        
        if not selected_file:
            st.warning("Vui lòng chọn file dữ liệu từ sidebar")
            return None
        
        st.info(f"Đang tải file: {selected_file}")
        
        df = load_csv_from_minio(client, MINIO_CLEAN_BUCKET, selected_file)
        
        if df is None:
            return None
            
        # Chuẩn hóa dữ liệu
        df = standardize_dataframe(df)
        
        st.success(f"Đã tải thành công {len(df):,} bản ghi từ MinIO")
        return df
        
    except Exception as e:
        st.error(f"Lỗi khi kết nối MinIO: {e}")
        st.error("Kiểm tra lại cấu hình MinIO hoặc đảm bảo server đang chạy")
        return None

def standardize_dataframe(df):
    """Chuẩn hóa DataFrame từ MinIO"""
    try:
        # Xử lý cột thời gian
        if 'ts_utc' in df.columns:
            df['date'] = pd.to_datetime(df['ts_utc'], errors='coerce')
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        else:
            st.warning("Không tìm thấy cột thời gian (ts_utc hoặc date)")
            
        # Tạo cột month và year nếu có cột date
        if 'date' in df.columns:
            df['month'] = df['date'].dt.month
            df['year'] = df['date'].dt.year
        
        # Tạo location_key nếu chưa có
        if 'location_key' not in df.columns:
            if 'location' in df.columns:
                df['location_key'] = df['location']
            else:
                # Tạo location_key giả định
                df['location_key'] = 'LOC001'
                st.info("ℹTạo location_key mặc định: LOC001")
        
        # Kiểm tra và tạo các cột cần thiết
        required_columns = ['aqi', 'pm25', 'pm10', 'no2', 'so2', 'co', 'o3']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.warning(f"Thiếu các cột: {missing_columns}")
            # Không tạo dữ liệu giả, chỉ thông báo
        
        # Tạo các cột AQI phụ nếu có đủ dữ liệu gốc
        aqi_mappings = {
            'aqi_pm25': ('pm25', 2.1),
            'aqi_pm10': ('pm10', 1.2), 
            'aqi_no2': ('no2', 1.8),
            'aqi_o3': ('o3', 1.1),
            'aqi_so2': ('so2', 2.5),
            'aqi_co': ('co', 25)
        }
        
        for aqi_col, (source_col, multiplier) in aqi_mappings.items():
            if source_col in df.columns and aqi_col not in df.columns:
                df[aqi_col] = df[source_col] * multiplier
        
        # Thêm các cột bổ sung nếu chưa có
        additional_cols = ['aod', 'dust', 'uv_index', 'co2']
        for col in additional_cols:
            if col not in df.columns:
                # Không tạo dữ liệu giả, chỉ thông báo thiếu
                pass
        
        st.info(f"Các cột có sẵn: {list(df.columns)}")
        return df
        
    except Exception as e:
        st.error(f"Lỗi khi chuẩn hóa dữ liệu: {e}")
        return df

def phan_vung_o_nhiem(df):
    """PHÂN VÙNG Ô NHIỄM & XẾP HẠNG ĐỊA ĐIỂM"""
    st.header("PHÂN VÙNG Ô NHIỄM & XẾP HẠNG ĐỊA ĐIỂM")

    # Tính toán ranking theo month + year
    df_rank = (
        df.groupby(['location_key', 'month', 'year'])
        .agg(
            avg_aqi=('aqi', 'mean'),
            max_aqi=('aqi', 'max'),
            avg_pm25=('pm25', 'mean'),
            median_pm25=('pm25', 'median'),
            exceedance_days=('aqi', lambda x: (x > 100).sum()),
            data_completeness=('aqi', lambda x: x.count()/len(x)*100)
        )
        .reset_index()
    )

    df_rank['air_quality_score'] = (100 - df_rank['avg_aqi']/500*100) * (df_rank['data_completeness']/100)
    df_rank['rank'] = df_rank['avg_aqi'].rank(method='dense').astype(int)

    # Summary ranking table (toàn bộ địa điểm)
    st.subheader("Bảng Xếp Hạng Tổng Thể Tất Cả Địa Điểm")
    ranking_summary_all = (
        df_rank.groupby('location_key')
        .agg({
            'avg_aqi': 'mean',
            'air_quality_score': 'mean',
            'exceedance_days': 'sum'
        })
        .round(2)
        .sort_values('avg_aqi')
    )
    ranking_summary_all['rank'] = range(1, len(ranking_summary_all) + 1)
    st.dataframe(ranking_summary_all, use_container_width=True)

    # Top 10 Rankings
    st.subheader("Top 10 Địa Điểm Theo Chất Lượng Không Khí")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top 10 địa điểm ô nhiễm nhất
        st.write("**Top 10 Địa Điểm Ô Nhiễm Nặng Nhất**")
        top_polluted = ranking_summary_all.nlargest(10, 'avg_aqi')[['avg_aqi', 'air_quality_score', 'exceedance_days']]
        top_polluted.columns = ['AQI TB', 'Điểm Chất Lượng', 'Ngày Vượt Chuẩn']
        
        # Thêm icon và màu sắc cảnh báo
        top_polluted_display = top_polluted.copy()
        top_polluted_display.index = [f"{idx}" for idx in top_polluted_display.index]
        
        st.dataframe(
            top_polluted_display.style.background_gradient(subset=['AQI TB'], cmap='Reds'),
            use_container_width=True
        )
    
    with col2:
        # Top 10 địa điểm sạch nhất
        st.write("**Top 10 Địa Điểm Không Khí Sạch Nhất**")
        top_clean = ranking_summary_all.nsmallest(10, 'avg_aqi')[['avg_aqi', 'air_quality_score', 'exceedance_days']]
        top_clean.columns = ['AQI TB', 'Điểm Chất Lượng', 'Ngày Vượt Chuẩn']
        
        # Thêm icon và màu sắc tích cực
        top_clean_display = top_clean.copy()
        top_clean_display.index = [f"{idx}" for idx in top_clean_display.index]
        
        st.dataframe(
            top_clean_display.style.background_gradient(subset=['AQI TB'], cmap='Greens_r'),
            use_container_width=True
        )
    
    # Thống kê so sánh
    st.write("**So Sánh Giữa Hai Nhóm:**")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        avg_polluted = top_polluted['AQI TB'].mean()
        avg_clean = top_clean['AQI TB'].mean()
        difference = avg_polluted - avg_clean
        st.metric(
            "Chênh Lệch AQI TB",
            f"{difference:.1f}",
            delta=f"{(difference/avg_clean*100):+.1f}%"
        )
    
    with col2:
        max_exceedance = top_polluted['Ngày Vượt Chuẩn'].max()
        min_exceedance = top_clean['Ngày Vượt Chuẩn'].min()
        st.metric(
            "Ngày Vượt Chuẩn (Max vs Min)",
            f"{max_exceedance:.0f} vs {min_exceedance:.0f}",
            delta=f"Chênh {max_exceedance-min_exceedance:.0f} ngày"
        )
    
    with col3:
        quality_diff = top_clean['Điểm Chất Lượng'].mean() - top_polluted['Điểm Chất Lượng'].mean()
        st.metric(
            "Điểm Chất Lượng (Sạch - Ô nhiễm)",
            f"{quality_diff:+.1f}",
            delta="Sạch hơn" if quality_diff > 0 else "Kém hơn"
        )

    # Sidebar / selection: chọn tỉnh(s) và năm để hiển thị biểu đồ
    available_locations = sorted(df_rank['location_key'].unique())
    selected_locations = st.multiselect("Chọn địa điểm để tiến hành phân tích chi tiết:", options=available_locations, default=available_locations[:5])

    available_years = sorted(df_rank['year'].unique())
    selected_years = st.multiselect("Chọn năm để hiển thị:", options=available_years, default=available_years)

    # Lọc df_rank theo lựa chọn (chỉ cho biểu đồ)
    df_rank_filt = df_rank[df_rank['location_key'].isin(selected_locations) & df_rank['year'].isin(selected_years)]

    # Summary cho biểu đồ
    ranking_summary = (
        df_rank_filt.groupby('location_key')
        .agg({
            'avg_aqi': 'mean',
            'air_quality_score': 'mean',
            'exceedance_days': 'sum'
        })
        .round(2)
        .sort_values('avg_aqi')
    )

    # Phân vùng ô nhiễm theo mức độ
    st.subheader("Phân Vùng Ô Nhiễm Theo Mức Độ")
    
    if len(df_rank_filt) == 0:
        st.info("Không có dữ liệu cho lựa chọn này.")
    else:
        # Phân loại các vùng theo mức độ ô nhiễm
        def classify_pollution_zone(aqi):
            if aqi <= 50: return "🟢 Tốt"
            elif aqi <= 100: return "🟡 Trung Bình"
            elif aqi <= 150: return "🟠 Không Tốt Cho Nhóm Nhạy Cảm"
            elif aqi <= 200: return "🔴 Không Tốt"
            elif aqi <= 300: return "🟣 Rất Không Tốt"
            else: return "⚫ Nguy Hiểm"
        
        # Tính phân vùng cho từng địa điểm
        zone_analysis = (
            df_rank_filt.groupby('location_key')['avg_aqi']
            .mean()
            .reset_index()
        )
        zone_analysis['pollution_zone'] = zone_analysis['avg_aqi'].apply(classify_pollution_zone)
        zone_analysis = zone_analysis.sort_values('avg_aqi')
        col1, col2 = st.columns(2)
        
        with col1:
            # Biểu đồ phân bố vùng ô nhiễm
            zone_counts = zone_analysis['pollution_zone'].value_counts()
            
            fig_zone = px.pie(
                values=zone_counts.values,
                names=zone_counts.index,
                title="Phân Bố Các Vùng Ô Nhiễm",
                color_discrete_sequence=['#009966', '#ffde33', '#ff9933', '#cc0033', '#660099', '#7e0023']
            )
            st.plotly_chart(fig_zone, use_container_width=True)

        with col2:
            # Biểu đồ AQI theo từng địa điểm với màu sắc theo vùng
            zone_colors = {
                "🟢 Tốt": "#009966",
                "🟡 Trung Bình": "#ffde33",
                "🟠 Không Tốt Cho Nhóm Nhạy Cảm": "#ff9933",
                "🔴 Không Tốt": "#cc0033",
                "🟣 Rất Không Tốt": "#660099",
                "⚫ Nguy Hiểm": "#7e0023"
            }
            
            zone_analysis['color'] = zone_analysis['pollution_zone'].map(zone_colors)
            
            fig_aqi = px.bar(
                zone_analysis,
                x='location_key',
                y='avg_aqi',
                color='pollution_zone',
                color_discrete_map=zone_colors,
                title="AQI Trung Bình Theo Địa Điểm & Vùng Ô Nhiễm"
            )
            
            # Thêm đường ngưỡng với annotation được cải thiện
            fig_aqi.add_hline(
                y=50, line_dash="dash", line_color="#009966",
                annotation_text="<b>Tốt (50)</b>", 
                annotation_position="right",
                annotation=dict(
                    font_size=12, 
                    font_color="white",
                    bgcolor="#009966",
                    borderpad=5,
                    xshift=5
                )
            )
            fig_aqi.add_hline(
                y=100, line_dash="dash", line_color="#ffde33",
                annotation_text="<b>Trung Bình (100)</b>", 
                annotation_position="right",
                annotation=dict(
                    font_size=12, 
                    font_color="black",
                    bgcolor="#ffde33",
                    borderpad=5,
                    xshift=5
                )
            )
            fig_aqi.add_hline(
                y=150, line_dash="dash", line_color="#ff9933",
                annotation_text="<b>Không Tốt Nhóm Nhạy Cảm (150)</b>", 
                annotation_position="right",
                annotation=dict(
                    font_size=12, 
                    font_color="white",
                    bgcolor="#ff9933",
                    borderpad=5,
                    xshift=5
                )
            )
            fig_aqi.add_hline(
                y=200, line_dash="dash", line_color="#cc0033",
                annotation_text="<b>Không Tốt (200)</b>", 
                annotation_position="right",
                annotation=dict(
                    font_size=12, 
                    font_color="white",
                    bgcolor="#cc0033",
                    borderpad=5,
                    xshift=5
                )
            )
            fig_aqi.add_hline(
                y=300, line_dash="dash", line_color="#660099",
                annotation_text="<b>Rất Không Tốt (300)</b>", 
                annotation_position="right",
                annotation=dict(
                    font_size=12, 
                    font_color="white",
                    bgcolor="#660099",
                    borderpad=5,
                    xshift=5
                )
            )
            fig_aqi.update_layout(
                xaxis_tickangle=-45,
                legend=dict(
                    orientation="v",
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=1.25
                ),
                margin=dict(r=250)
            )
            st.plotly_chart(fig_aqi, use_container_width=True)
        

    # Heatmap: use year-month as x axis to show month across years
    st.subheader("Heatmap AQI Theo Tháng (kèm Năm) và Địa Điểm")
    # build period string
    df_rank_filt['period'] = df_rank_filt['year'].astype(str) + '-' + df_rank_filt['month'].astype(str).str.zfill(2)
    pivot_data = df_rank_filt.pivot_table(values='avg_aqi', index='location_key', columns='period', aggfunc='mean')

    if pivot_data.empty:
        st.info("Không có dữ liệu heatmap cho lựa chọn này.")
    else:
        # If too many locations selected, restrict to top 20 for readability
        max_locations = 20
        if len(pivot_data) > max_locations:
            st.warning(f"Quá nhiều địa điểm ({len(pivot_data)}). Hiển thị top {max_locations} theo AQI trung bình.")
            display_idx = ranking_summary.head(max_locations).index
            pivot_display = pivot_data.loc[pivot_data.index.intersection(display_idx)]
        else:
            pivot_display = pivot_data

        # Ensure columns sorted chronologically
        pivot_display = pivot_display.reindex(sorted(pivot_display.columns), axis=1)

        fig_heatmap = px.imshow(
            pivot_display.values,
            labels=dict(x="Thời Gian (YYYY-MM)", y="Địa Điểm", color="AQI Trung Bình"),
            x=pivot_display.columns,
            y=pivot_display.index,
            color_continuous_scale="RdYlGn_r"
        )
        fig_heatmap.update_layout(height=400 + 20*len(pivot_display))
        st.plotly_chart(fig_heatmap, use_container_width=True)

    return df_rank

def chat_o_nhiem(df):
    """PHÂN TÍCH CHẤT Ô NHIỄM CHÍNH ẢNH HƯỞNG"""
    st.header("PHÂN TÍCH CHẤT Ô NHIỄM CHÍNH ẢNH HƯỞNG")
    
    # Tính các tỷ lệ chất ô nhiễm
    df_pollutant = df.copy()
    
    # Các tỷ lệ quan trọng trong phân tích chất lượng không khí
    pollutant_ratios = {}
    if 'pm25' in df.columns and 'pm10' in df.columns:
        pollutant_ratios['pm25_pm10_ratio'] = df_pollutant['pm25'] / df_pollutant['pm10']
    if 'no2' in df.columns and 'so2' in df.columns:
        pollutant_ratios['no2_so2_ratio'] = df_pollutant['no2'] / df_pollutant['so2']
    if 'co' in df.columns and 'no2' in df.columns:
        pollutant_ratios['co_no2_ratio'] = df_pollutant['co'] / df_pollutant['no2']
    if 'dust' in df.columns and 'pm25' in df.columns:
        pollutant_ratios['dust_pm25_ratio'] = df_pollutant['dust'] / df_pollutant['pm25']
    
    for ratio_name, ratio_values in pollutant_ratios.items():
        df_pollutant[ratio_name] = ratio_values
    
    # Phân tích phân bố các chất ô nhiễm chi tiết
    st.subheader("Phân Bố Các Chất Ô Nhiễm")
    
    # Hiển thị thông tin về các cột AQI có sẵn
    aqi_cols = ['aqi_pm25', 'aqi_pm10', 'aqi_no2', 'aqi_o3', 'aqi_so2', 'aqi_co']
    available_aqi_cols = [col for col in aqi_cols if col in df_pollutant.columns]
    
    st.info(f"Các chỉ số AQI có sẵn: {', '.join(available_aqi_cols)}")
    
    if available_aqi_cols:
        # Tính chất ô nhiễm chính và thống kê đầy đủ
        df_pollutant['dominant_pollutant'] = df_pollutant[available_aqi_cols].idxmax(axis=1).str.replace('aqi_', '').str.upper()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Phân tích phân bố các chất ô nhiễm theo nhiều tiêu chí
            
            # Cho phép người dùng chọn ngưỡng
            st.write("**Tùy chọn phân tích:**")
            aqi_threshold = st.selectbox(
                "Chọn ngưỡng AQI để phân tích:",
                options=[100, 150, 200],
                index=0,
                format_func=lambda x: f"AQI > {x} ({'Trung bình' if x==100 else 'Kém' if x==150 else 'Rất kém'})"
            )
            
            # Tần suất vượt ngưỡng (cách phân tích hợp lý)
            threshold_exceed = {}
            
            for col in available_aqi_cols:
                pollutant_name = col.replace('aqi_', '').upper()
                exceed_count = (df_pollutant[col] > aqi_threshold).sum()
                threshold_exceed[pollutant_name] = exceed_count
            
            total_exceed = sum(threshold_exceed.values())
            
            if total_exceed == 0:
                st.warning(f"Không có chất ô nhiễm nào vượt ngưỡng AQI > {aqi_threshold}")
                exceed_percentages = {k: 0 for k in threshold_exceed.keys()}
            else:
                exceed_percentages = {k: (v/total_exceed*100) for k, v in threshold_exceed.items()}
                st.info(f"Tổng số lần vượt ngưỡng: {total_exceed:,} lần")
            
          
            fig_pie = px.pie(
                values=list(exceed_percentages.values()),
                names=list(exceed_percentages.keys()),
                title=f"% Tần Suất Vượt Ngưỡng AQI > {aqi_threshold}",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            
            # Tùy chỉnh hiển thị để thấy rõ các phần nhỏ
            fig_pie.update_traces(
                textposition='auto',  # Tự động điều chỉnh vị trí text
                texttemplate='%{label}<br>%{value:.5f}%',  # Hiển thị 5 chữ số thập phân
                textfont_size=10,
                pull=[0.1 if v < 1 else 0 for v in exceed_percentages.values()]  # Kéo ra các phần nhỏ
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
            # Hiển thị bảng số liệu chi tiết
            st.write(f"**Phân Tích Vượt Ngưỡng AQI > {aqi_threshold}:**")
            exceed_df = pd.DataFrame({
                'Chất Ô Nhiễm': list(exceed_percentages.keys()),
                f'Số Lần > {aqi_threshold}': list(threshold_exceed.values()),
                '% Tần Suất': [f"{v:.5f}%" for v in exceed_percentages.values()],
                'AQI Trung Bình': [df_pollutant[f'aqi_{k.lower()}'].mean().round(2) for k in exceed_percentages.keys()]
            })
            st.dataframe(exceed_df, use_container_width=True, hide_index=True)
            
 
        
        with col2:
            # Biểu đồ cột cho tất cả chất ô nhiễm (giá trị trung bình AQI)
            aqi_means = df_pollutant[available_aqi_cols].mean()
            pollutant_names = [col.replace('aqi_', '').upper() for col in available_aqi_cols]
            
            fig_bar = px.bar(
                x=pollutant_names,
                y=aqi_means.values,
                title="Mức AQI Trung Bình Tất Cả Chất Ô Nhiễm",
                color=aqi_means.values,
                color_continuous_scale='Reds'
            )
            fig_bar.update_layout(
                xaxis_title="Chất Ô Nhiễm",
                yaxis_title="AQI Trung Bình",
                showlegend=False
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        
        # Thống kê chi tiết tất cả chất ô nhiễm theo ngưỡng
        st.subheader("Thống Kê Chi Tiết Theo Ngưỡng")
        
        col3, col4 = st.columns(2)
        
        with col3:
            # Bảng thống kê theo ngưỡng đã chọn (sử dụng lại aqi_threshold)
            st.write(f"**Phân Tích Theo Ngưỡng AQI > {aqi_threshold}:**")
            
            threshold_details = []
            for col in available_aqi_cols:
                pollutant_name = col.replace('aqi_', '').upper()
                exceed_count = (df_pollutant[col] > aqi_threshold).sum()
                total_records = len(df_pollutant)
                percentage = (exceed_count / total_records * 100) if total_records > 0 else 0
                
                threshold_details.append({
                    'Chất Ô Nhiễm': pollutant_name,
                    'Số Lần Vượt': exceed_count,
                    'Tổng Số Mẫu': total_records,
                    '% Vượt Ngưỡng': f"{percentage:.4f}%"
                })
            
            threshold_df = pd.DataFrame(threshold_details)
            
            # Thêm mô tả
            pollutant_descriptions = {
                'PM25': 'Bụi mịn < 2.5μm',
                'PM10': 'Bụi mịn < 10μm', 
                'NO2': 'Nitro dioxide',
                'O3': 'Ozone',
                'SO2': 'Lưu huỳnh dioxide',
                'CO': 'Carbon monoxide'
            }
            threshold_df['Mô Tả'] = threshold_df['Chất Ô Nhiễm'].map(pollutant_descriptions).fillna('Không rõ')
            
            st.dataframe(threshold_df, use_container_width=True, hide_index=True)
        
        with col4:
            # Bảng thống kê AQI trung bình và so sánh với ngưỡng
            st.write("**Mức AQI Trung Bình So Với Ngưỡng:**")
            
            all_aqi_stats = pd.DataFrame({
                'Chất Ô Nhiễm': [col.replace('aqi_', '').upper() for col in available_aqi_cols],
                'AQI TB': [df_pollutant[col].mean().round(2) for col in available_aqi_cols],
                'AQI Max': [df_pollutant[col].max().round(2) for col in available_aqi_cols],
                'Ngưỡng': aqi_threshold
            })
            
            # Thêm đánh giá so với ngưỡng
            def compare_with_threshold(avg_val, threshold_val):
                if avg_val < threshold_val * 0.5: return "Rất tốt"
                elif avg_val < threshold_val: return "Chấp nhận"
                elif avg_val < threshold_val * 1.5: return "Vượt ngưỡng"
                else: return "Nguy hiểm"
            
            all_aqi_stats['So Sánh'] = all_aqi_stats.apply(
                lambda row: compare_with_threshold(row['AQI TB'], row['Ngưỡng']), 
                axis=1
            )
            
            st.dataframe(all_aqi_stats, use_container_width=True, hide_index=True)
        
 
    else:
        st.warning("Không tìm thấy dữ liệu AQI cho các chất ô nhiễm")
    
    # Phân tích mức độ của từng chất ô nhiễm
    st.subheader("Mức Độ Các Chất Ô Nhiễm Theo Địa Điểm")
    
    # Các chất ô nhiễm chính cần phân tích
    main_pollutants = ['pm25', 'pm10', 'no2', 'so2', 'co', 'o3']
    available_pollutants = [p for p in main_pollutants if p in df_pollutant.columns]
    
    if available_pollutants:
        # Thêm tùy chọn lọc địa điểm
        st.write("**Lọc địa điểm để phân tích:**")
        all_locations = sorted(df_pollutant['location_key'].unique())
        selected_locations_pollutant = st.multiselect(
            "Chọn địa điểm để hiển thị:", 
            options=all_locations, 
            default=all_locations[:5],  # Mặc định 5 địa điểm đầu tiên
            key="pollutant_location_filter"
        )
        
        # Nếu không chọn gì thì hiển thị tất cả
        if not selected_locations_pollutant:
            selected_locations_pollutant = all_locations
        
        # Lọc dữ liệu theo địa điểm đã chọn
        df_pollutant_filtered = df_pollutant[df_pollutant['location_key'].isin(selected_locations_pollutant)]
        
        # Tính giá trị trung bình theo địa điểm
        pollutant_by_location = df_pollutant_filtered.groupby('location_key')[available_pollutants].mean().reset_index()
        
        # Hiển thị thông tin số lượng địa điểm đã chọn
        st.info(f"Đang hiển thị {len(selected_locations_pollutant)} / {len(all_locations)} địa điểm")
        
        # Tạo biểu đồ riêng cho từng chất ô nhiễm để tránh chồng chéo
        cols_per_row = 2  # 2 cột mỗi hàng
        for i in range(0, len(available_pollutants), cols_per_row):
            cols = st.columns(cols_per_row)
            
            for j in range(cols_per_row):
                if i + j < len(available_pollutants):
                    pollutant = available_pollutants[i + j]
                    
                    with cols[j]:
                        # Tạo biểu đồ riêng cho từng chất
                        fig_single = px.bar(
                            pollutant_by_location,
                            x='location_key',
                            y=pollutant,
                            title=f"Mức {pollutant.upper()} Trung Bình",
                            color=pollutant,
                            color_continuous_scale='Reds'
                        )
                        
                        fig_single.update_layout(
                            height=400,
                            xaxis_tickangle=-45,
                            xaxis_title="Địa Điểm",
                            yaxis_title=f"{pollutant.upper()} (μg/m³)",
                            showlegend=False
                        )
                        
                        st.plotly_chart(fig_single, use_container_width=True)
    
    # Biểu đồ tỷ lệ các chất ô nhiễm (nếu có dữ liệu)
    if pollutant_ratios:
        st.subheader("Tỷ Lệ Giữa Các Chất Ô Nhiễm")
        
        # Sử dụng dữ liệu đã lọc theo địa điểm đã chọn
        ratio_data = df_pollutant_filtered.groupby('location_key')[list(pollutant_ratios.keys())].mean().reset_index()
        
        # Tên hiển thị thân thiện hơn
        ratio_titles = {
            'pm25_pm10_ratio': 'PM2.5/PM10',
            'no2_so2_ratio': 'NO2/SO2', 
            'co_no2_ratio': 'CO/NO2',
            'dust_pm25_ratio': 'Bụi/PM2.5'
        }
        
        available_ratios = [k for k in pollutant_ratios.keys() if k in ratio_data.columns]
        
        if available_ratios:
            # Tạo biểu đồ riêng cho từng tỷ lệ để tránh chồng chéo
            cols_per_row = 2  # 2 cột mỗi hàng
            colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD']
            
            for i in range(0, len(available_ratios), cols_per_row):
                cols = st.columns(cols_per_row)
                
                for j in range(cols_per_row):
                    if i + j < len(available_ratios):
                        ratio_col = available_ratios[i + j]
                        ratio_title = ratio_titles.get(ratio_col, ratio_col)
                        
                        with cols[j]:
                            # Tạo biểu đồ riêng cho từng tỷ lệ
                            fig_single_ratio = px.bar(
                                ratio_data,
                                x='location_key',
                                y=ratio_col,
                                title=f"Tỷ Lệ {ratio_title}",
                                color=ratio_col,
                                color_continuous_scale='Viridis'
                            )
                            
                            fig_single_ratio.update_layout(
                                height=400,
                                xaxis_tickangle=-45,
                                xaxis_title="Địa Điểm",
                                yaxis_title=f"Tỷ Lệ {ratio_title}",
                                showlegend=False
                            )
                            
                            st.plotly_chart(fig_single_ratio, use_container_width=True)
            
            # Giải thích ý nghĩa của các tỷ lệ
            with st.expander("Ý Nghĩa Của Các Tỷ Lệ"):
                st.markdown("""
                - **PM2.5/PM10**: Tỷ lệ bụi mịn siêu nhỏ so với bụi thông thường (càng cao càng nguy hiểm)
                - **NO2/SO2**: Tỷ lệ giữa khí thải giao thông và công nghiệp
                - **CO/NO2**: Chỉ số đốt cháy không hoàn toàn
                - **Bụi/PM2.5**: Tỷ lệ bụi tự nhiên và bụi nhân tạo
                """)
    
    # Thống kê tổng quát về mức độ ô nhiễm
    st.subheader("Đánh Giá Tổng Quát Mức Độ Ô Nhiễm")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'aqi' in df.columns:
            avg_aqi = df['aqi'].mean()
            aqi_status = "Tốt" if avg_aqi <= 50 else "Trung bình" if avg_aqi <= 100 else "Kém"
            st.metric("AQI Trung Bình", f"{avg_aqi:.1f}", delta=aqi_status)
    
    with col2:
        if 'pm25' in df.columns:
            avg_pm25 = df['pm25'].mean()
            pm25_status = "An toàn" if avg_pm25 <= 12 else "Cảnh báo" if avg_pm25 <= 35 else "Nguy hiểm"
            st.metric("PM2.5 Trung Bình (μg/m³)", f"{avg_pm25:.1f}", delta=pm25_status)
    
    with col3:
        if available_aqi_cols:
            most_common = df_pollutant['dominant_pollutant'].mode()[0]
            st.metric("Chất Ô Nhiễm Chính", most_common, delta="Thường xuyên nhất")
    
    return df_pollutant

def phan_tich_mua_vu(df):
    """PHÂN TÍCH MÙA VỤ & CHU KỲ THEO THỜI GIAN"""
    st.header("PHÂN TÍCH MÙA VỤ & CHU KỲ THEO THỜI GIAN")
    
    # Cho phép người dùng chọn chất ô nhiễm để phân tích
    st.subheader("Tùy Chọn Phân Tích")
    
    pollutant_options = {
        'pm25': 'PM2.5 (Bụi mịn < 2.5μm)',
        'pm10': 'PM10 (Bụi mịn < 10μm)',
        'no2': 'NO2 (Nitro dioxide)',
        'so2': 'SO2 (Lưu huỳnh dioxide)',
        'co': 'CO (Carbon monoxide)',
        'o3': 'O3 (Ozone)',
        'aqi': 'AQI (Chỉ số chất lượng không khí)'
    }
    
    # Lọc chỉ những chất có sẵn trong dữ liệu
    available_pollutants = {k: v for k, v in pollutant_options.items() if k in df.columns}
    
    if not available_pollutants:
        st.error("Không tìm thấy dữ liệu chất ô nhiễm để phân tích!")
        return None
    
    selected_pollutant = st.selectbox(
        "Chọn chất ô nhiễm để phân tích mùa vụ:",
        options=list(available_pollutants.keys()),
        format_func=lambda x: available_pollutants[x],
        index=0,
        help="Chọn chất ô nhiễm để xem biến động theo mùa"
    )
    
    st.info(f"Đang phân tích mùa vụ cho: **{available_pollutants[selected_pollutant]}**")
    st.markdown("---")
    
    # Tạo DataFrame với seasonal info
    df_seasonal = df.copy()
    
    # Phân loại mùa theo tháng (Việt Nam)
    def get_season(month):
        if month in [12, 1, 2]:
            return 'Mua Dong (Dec-Feb)'
        elif month in [3, 4, 5]:
            return 'Mua Xuan (Mar-May)'
        elif month in [6, 7, 8]:
            return 'Mua He (Jun-Aug)'
        else:  # 9, 10, 11
            return 'Mua Thu (Sep-Nov)'
    
    df_seasonal['season'] = df_seasonal['month'].apply(get_season)
    
    # So sánh theo mùa
    st.subheader("So Sánh Mức Độ Ô Nhiễm Theo Mùa")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Bar chart so sánh trung bình theo mùa
        seasonal_avg = df_seasonal.groupby('season')[selected_pollutant].agg(['mean', 'std']).reset_index()
        
        # Sắp xếp theo thứ tự mùa
        season_order = ['Mua Dong (Dec-Feb)', 'Mua Xuan (Mar-May)', 
                       'Mua He (Jun-Aug)', 'Mua Thu (Sep-Nov)']
        seasonal_avg['season'] = pd.Categorical(seasonal_avg['season'], categories=season_order, ordered=True)
        seasonal_avg = seasonal_avg.sort_values('season')
        
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            x=seasonal_avg['season'],
            y=seasonal_avg['mean'],
            error_y=dict(type='data', array=seasonal_avg['std']),
            marker_color=['#87CEEB', '#90EE90', '#FFD700', '#FFA07A'],
            text=seasonal_avg['mean'].round(2),
            textposition='outside'
        ))
        
        fig_bar.update_layout(
            title=f"Mức Trung Bình {selected_pollutant.upper()} Theo Mùa",
            xaxis_title="Mùa",
            yaxis_title=f"{selected_pollutant.upper()} (µg/m³)",
            showlegend=False,
            height=500
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    with col2:
        # Thống kê theo mùa
        seasonal_stats = df_seasonal.groupby('season')[selected_pollutant].agg([
            ('Trung Bình', 'mean'),
            ('Trung Vị', 'median'),
            ('Min', 'min'),
            ('Max', 'max'),
            ('Độ Lệch Chuẩn', 'std')
        ]).round(2)
        
        # Sắp xếp theo thứ tự mùa
        season_order = ['Mua Dong (Dec-Feb)', 'Mua Xuan (Mar-May)', 
                       'Mua He (Jun-Aug)', 'Mua Thu (Sep-Nov)']
        seasonal_stats = seasonal_stats.reindex(season_order)
        
        st.write("**Thống Kê Chi Tiết Theo Mùa:**")
        st.dataframe(seasonal_stats, use_container_width=True)
        
        # Tìm mùa ô nhiễm nhất/sạch nhất
        most_polluted_season = seasonal_stats['Trung Bình'].idxmax()
        cleanest_season = seasonal_stats['Trung Bình'].idxmin()
        
        st.success(f"**Mùa ô nhiễm nhất:** {most_polluted_season}")
        st.success(f"**Mùa sạch nhất:** {cleanest_season}")
    

    # Phân tích theo tháng (Line chart)
    st.subheader("Biến Động Theo Tháng")
    
    selected_locations = st.multiselect(
        "Chọn địa điểm để so sánh:",
        options=sorted(df_seasonal['location_key'].unique()),
        default=sorted(df_seasonal['location_key'].unique())[:3],
        key="seasonal_location_select"
    )
    
    if selected_locations:
        monthly_location = df_seasonal[df_seasonal['location_key'].isin(selected_locations)].groupby(
            ['location_key', 'month', 'season']
        )[selected_pollutant].mean().reset_index()
        
        fig_line = px.line(
            monthly_location,
            x='month',
            y=selected_pollutant,
            color='location_key',
            title=f"Biến Động {selected_pollutant.upper()} Theo Tháng",
            markers=True
        )
        
        # Thêm vùng màu theo mùa
        seasons_ranges = [
            (12, 2, 'Mua Dong', 'rgba(135, 206, 235, 0.2)'),
            (3, 5, 'Mua Xuan', 'rgba(144, 238, 144, 0.2)'),
            (6, 8, 'Mua He', 'rgba(255, 215, 0, 0.2)'),
            (9, 11, 'Mua Thu', 'rgba(255, 160, 122, 0.2)')
        ]
        
        for start, end, name, color in seasons_ranges:
            if start <= end:
                fig_line.add_vrect(
                    x0=start-0.5, x1=end+0.5,
                    fillcolor=color, opacity=0.3,
                    layer="below", line_width=0,
                    annotation_text=name, annotation_position="top left"
                )
        
        fig_line.update_layout(
            xaxis_title="Tháng",
            yaxis_title=f"{selected_pollutant.upper()} (µg/m³)",
            height=500,
            xaxis=dict(tickmode='linear', dtick=1)
        )
        st.plotly_chart(fig_line, use_container_width=True)
    
    # Heatmap theo tháng × năm để thấy chu kỳ mùa vụ lặp lại
    st.subheader("Chu Kỳ Mùa Vụ Theo Thời Gian")
    
    st.info("Heatmap này giúp bạn nhìn thấy pattern mùa vụ lặp lại qua các năm - mỗi tháng trong năm có mức ô nhiễm như thế nào")
    
    # Tính trung bình theo tháng và năm
    monthly_yearly = df_seasonal.groupby(['year', 'month'])[selected_pollutant].mean().reset_index()
    
    # Tạo pivot table: year × month
    pivot_cycle = monthly_yearly.pivot(index='year', columns='month', values=selected_pollutant)
    
    if not pivot_cycle.empty:
        fig_heatmap_cycle = px.imshow(
            pivot_cycle.values,
            labels=dict(x="Tháng", y="Năm", color=f"{selected_pollutant.upper()}"),
            x=[f"T{m}" for m in pivot_cycle.columns],
            y=pivot_cycle.index,
            color_continuous_scale="RdYlGn_r",
            aspect="auto",
            title=f"Chu Kỳ Mùa Vụ: {selected_pollutant.upper()} Theo Tháng × Năm"
        )
        
        # Thêm vùng màu mùa
        fig_heatmap_cycle.update_layout(
            height=300 + 40*len(pivot_cycle),
            xaxis=dict(
                tickmode='linear',
                tick0=0,
                dtick=1
            )
        )
        
        st.plotly_chart(fig_heatmap_cycle, use_container_width=True)
        
        # Phân tích pattern mùa vụ
        st.write("**Nhận Xét Về Chu Kỳ Mùa Vụ:**")
        
        # Tính trung bình từng tháng qua các năm
        monthly_pattern = df_seasonal.groupby('month')[selected_pollutant].mean().sort_values(ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            top_3_months = monthly_pattern.head(3)
            st.write("**Top 3 Tháng Ô Nhiễm Nhất:**")
            for month, value in top_3_months.items():
                season = df_seasonal[df_seasonal['month'] == month]['season'].iloc[0]
                st.write(f"- Tháng {month} ({season}): {value:.2f} µg/m³")
        
        with col2:
            bottom_3_months = monthly_pattern.tail(3)
            st.write("**Top 3 Tháng Sạch Nhất:**")
            for month, value in bottom_3_months.items():
                season = df_seasonal[df_seasonal['month'] == month]['season'].iloc[0]
                st.write(f"- Tháng {month} ({season}): {value:.2f} µg/m³")
    else:
        st.warning("Không đủ dữ liệu để hiển thị chu kỳ mùa vụ")
    
    return df_seasonal

@st.cache_data(ttl=3600)
def calculate_feature_importance(df, target, available_features):
    """Tính toán feature importance với cache để tăng tốc"""
    # Chuẩn bị dữ liệu
    df_clean = df[available_features + [target]].dropna()
    
    if len(df_clean) < 100:
        return None, f"Chỉ có {len(df_clean)} mẫu"
    
    X = df_clean[available_features]
    y = df_clean[target]
    
    # Sử dụng cả 2 phương pháp
    selector_f = SelectKBest(score_func=f_regression, k='all')
    selector_f.fit(X, y)
    
    selector_mi = SelectKBest(score_func=mutual_info_regression, k='all')
    selector_mi.fit(X, y)
    
    # Chuẩn hóa scores
    f_scores_raw = selector_f.scores_
    f_scores_norm = (f_scores_raw - f_scores_raw.min()) / (f_scores_raw.max() - f_scores_raw.min()) * 100
    
    mi_scores_raw = selector_mi.scores_
    mi_scores_norm = (mi_scores_raw - mi_scores_raw.min()) / (mi_scores_raw.max() - mi_scores_raw.min()) * 100
    
    # Tạo DataFrame kết quả
    importance_df = pd.DataFrame({
        'Chất Ô Nhiễm': available_features,
        'Điểm Tuyến Tính': f_scores_norm,
        'Điểm Phi Tuyến': mi_scores_norm
    })
    
    importance_df['Điểm Trung Bình'] = (importance_df['Điểm Tuyến Tính'] + importance_df['Điểm Phi Tuyến']) / 2
    importance_df = importance_df.sort_values('Điểm Trung Bình', ascending=False)
    importance_df['Xếp Hạng'] = range(1, len(importance_df) + 1)
    
    return importance_df, len(df_clean)

@st.cache_data
def calculate_correlations(df):
    """Tính ma trận tương quan với cache để tối ưu tốc độ"""
    pollutants = ['pm25','pm10','o3','no2','so2','co','aod','dust','uv_index','co2']
    available_pollutants = [p for p in pollutants if p in df.columns]
    
    if len(available_pollutants) < 2:
        return pd.DataFrame(), pd.DataFrame()
    
    # Tính ma trận tương quan tổng thể trước (nhanh nhất)
    overall_corr = df[available_pollutants].corr()
    
    # Tối ưu hóa tính toán chi tiết
    correlation_data = []
    
    # Chỉ tính cho các địa điểm có đủ dữ liệu
    location_counts = df['location_key'].value_counts()
    valid_locations = location_counts[location_counts >= 50].index  # Chỉ lấy địa điểm có ít nhất 50 bản ghi
    
    # Giới hạn số địa điểm để tránh quá chậm
    if len(valid_locations) > 20:
        valid_locations = valid_locations[:20]  # Chỉ lấy 20 địa điểm đầu
    
    with st.spinner(f"Đang tính toán tương quan cho {len(valid_locations)} địa điểm..."):
        # Sử dụng groupby thay vì nested loops
        for location in valid_locations:
            location_data = df[df['location_key'] == location]
            
            # Tính theo quý thay vì tháng để giảm tính toán
            location_data['quarter'] = location_data['month'].apply(lambda x: (x-1)//3 + 1)
            
            for quarter in location_data['quarter'].unique():
                subset = location_data[location_data['quarter'] == quarter]
                if len(subset) > 15:  # Tăng ngưỡng để giảm tính toán
                    try:
                        corr_matrix = subset[available_pollutants].corr()
                        
                        # Chỉ lấy tương quan mạnh để giảm kích thước dữ liệu
                        for i, p1 in enumerate(available_pollutants):
                            for j, p2 in enumerate(available_pollutants[i+1:], i+1):
                                corr_val = corr_matrix.iloc[i, j]
                                if not pd.isna(corr_val) and abs(corr_val) > 0.3:  # Chỉ lưu tương quan > 0.3
                                    correlation_data.append({
                                        'location_key': location,
                                        'quarter': quarter,
                                        'pollutant_1': p1,
                                        'pollutant_2': p2,
                                        'correlation': round(corr_val, 3)
                                    })
                    except:
                        continue  # Bỏ qua nếu có lỗi
    
    df_corr = pd.DataFrame(correlation_data)
    return overall_corr, df_corr

def ma_tran_tuong_quan(df):
    """MỐI QUAN HỆ GIỮA CÁC CHẤT & MA TRẬN TƯƠNG QUAN """
    st.header("MỐI QUAN HỆ CHẤT Ô NHIỄM & MA TRẬN TƯƠNG QUAN")
    # === PHẦN 1: PHÂN TÍCH MỐI QUAN HỆ ẢNH HƯỞNG ===
    st.header("MỐI QUAN HỆ ẢNH HƯỞNG GIỮA CÁC CHẤT")
    
    # Chọn target variable
    st.subheader("Tùy Chọn Phân Tích")
    
    target_options = {
        'pm25': 'PM2.5 (Bụi mịn < 2.5μm)',
        'pm10': 'PM10 (Bụi mịn < 10μm)',
        'aqi': 'AQI (Chỉ số chất lượng không khí)',
        'no2': 'NO2 (Nitro dioxide)',
        'o3': 'O3 (Ozone)'
    }
    
    available_targets = {k: v for k, v in target_options.items() if k in df.columns}
    
    if available_targets:
        target = st.selectbox(
            "Chọn biến mục tiêu (Target) để phân tích:",
            options=list(available_targets.keys()),
            format_func=lambda x: available_targets[x],
            index=0,
            help="Chọn chất ô nhiễm mà bạn muốn tìm hiểu yếu tố nào ảnh hưởng đến nó",
            key="feature_importance_target"
        )
        
        st.info(f"**Biến mục tiêu:** {available_targets[target]}")
        
        # Định nghĩa các features có thể dùng
        all_possible_features = ['pm25', 'pm10', 'no2', 'so2', 'co', 'o3', 'aod', 'dust', 'uv_index', 'co2',
                                 'aqi_pm25', 'aqi_pm10', 'aqi_no2', 'aqi_so2', 'aqi_co', 'aqi_o3']
        
        # Lọc features có sẵn và loại bỏ target
        available_features = [f for f in all_possible_features if f in df.columns and f != target]
        
        if len(available_features) >= 2:
            st.write(f"**Số lượng features có sẵn:** {len(available_features)}")
            
            # Chuẩn bị dữ liệu
            df_clean = df[available_features + [target]].dropna()
            
            if len(df_clean) >= 100:
                X = df_clean[available_features]
                y = df_clean[target]
                
                st.success(f"Đã chuẩn bị dữ liệu: {len(df_clean):,} mẫu × {len(available_features)} features")
                
                # Phương pháp CHÍNH: Tính điểm ảnh hưởng đơn giản
                st.subheader("Xếp Hạng Mức Độ Ảnh Hưởng")
                
                st.info(f"Đang tính toán xem chất ô nhiễm nào ảnh hưởng mạnh nhất đến **{available_targets[target]}**...")
                
                # Sử dụng cả 2 phương pháp để đánh giá toàn diện
                selector_f = SelectKBest(score_func=f_regression, k='all')
                selector_f.fit(X, y)
                
                selector_mi = SelectKBest(score_func=mutual_info_regression, k='all')
                selector_mi.fit(X, y)
                
                # Chuẩn hóa scores về [0, 100]
                f_scores_raw = selector_f.scores_
                f_scores_norm = (f_scores_raw - f_scores_raw.min()) / (f_scores_raw.max() - f_scores_raw.min()) * 100
                
                mi_scores_raw = selector_mi.scores_
                mi_scores_norm = (mi_scores_raw - mi_scores_raw.min()) / (mi_scores_raw.max() - mi_scores_raw.min()) * 100
                
                # Tính điểm trung bình
                importance_df = pd.DataFrame({
                    'Chất Ô Nhiễm': available_features,
                    'Điểm Tuyến Tính': f_scores_norm,
                    'Điểm Phi Tuyến': mi_scores_norm
                })
                
                importance_df['Điểm Trung Bình'] = (importance_df['Điểm Tuyến Tính'] + importance_df['Điểm Phi Tuyến']) / 2
                importance_df = importance_df.sort_values('Điểm Trung Bình', ascending=False)
                importance_df['Xếp Hạng'] = range(1, len(importance_df) + 1)
                
            # Hiển thị kết quả - Top 10
                fig_importance = px.bar(
                    importance_df.head(10),
                    x='Điểm Trung Bình',
                    y='Chất Ô Nhiễm',
                    orientation='h',
                    title=f"Top 10 Chất Ô Nhiễm Ảnh Hưởng Đến {target.upper()}",
                    color='Điểm Trung Bình',
                    color_continuous_scale='RdYlGn'
                )
                fig_importance.update_layout(
                    height=600,
                    yaxis={'categoryorder':'total ascending'},
                    xaxis_title="Điểm Ảnh Hưởng (0-100)",
                    yaxis_title=""
                )
                st.plotly_chart(fig_importance, use_container_width=True)
                
                # Bảng chi tiết đầy đủ
                with st.expander("Xem Bảng Chi Tiết Tất Cả Features"):
                    importance_display = importance_df.copy()
                    importance_display['Điểm Tuyến Tính'] = importance_display['Điểm Tuyến Tính'].round(1)
                    importance_display['Điểm Phi Tuyến'] = importance_display['Điểm Phi Tuyến'].round(1)
                    importance_display['Điểm Trung Bình'] = importance_display['Điểm Trung Bình'].round(1)
                    
                    st.dataframe(importance_display, use_container_width=True, hide_index=True)
            else:
                st.warning(f"Chỉ có {len(df_clean)} mẫu sau khi loại bỏ missing values. Cần ít nhất 100 mẫu để phân tích chính xác.")
        else:
            st.warning("Không đủ features để phân tích (cần ít nhất 2 features)")
    else:
        st.error("Không tìm thấy target variable phù hợp!")
    
    # === PHẦN 2: MA TRẬN TƯƠNG QUAN ===
    st.markdown("---")
    st.header("PHẦN 2: MA TRẬN TƯƠNG QUAN TỔNG QUAN")
    # Tính toán với cache
    overall_corr, df_corr = calculate_correlations(df)
    
    # Kiểm tra dữ liệu ngay từ đầu
    if overall_corr.empty:
        st.warning("Không đủ dữ liệu để tính ma trận tương quan tổng thể")
        return df_corr
    
    # Định nghĩa lại pollutants cho phần dưới
    pollutants = ['pm25','pm10','o3','no2','so2','co','aod','dust','uv_index','co2']
    
    # Hiển thị Ma Trận Tương Quan trước (tổng quan)
    st.subheader("Heatmap Tương Quan Tổng Thể")
    
    fig_overall = px.imshow(
        overall_corr.values,
        labels=dict(color="Hệ số tương quan"),
        x=overall_corr.columns,
        y=overall_corr.columns,
        color_continuous_scale="RdBu",
        zmin=-1, zmax=1,
        title="Ma Trận Tương Quan Tất Cả Chất Ô Nhiễm"
    )
    
    # Thêm annotations
    for i in range(len(overall_corr)):
        for j in range(len(overall_corr.columns)):
            fig_overall.add_annotation(
                x=j, y=i,
                text=str(round(overall_corr.iloc[i, j], 2)),
                showarrow=False,
                font=dict(color="black" if abs(overall_corr.iloc[i, j]) < 0.5 else "white")
            )
    
    st.plotly_chart(fig_overall, use_container_width=True)
    # Lưu kết quả lên MinIO
    st.subheader("Lưu Trữ Kết Quả")
    
    try:
        minio_client = Minio(
            MINIO_HOST,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        
        # Tạo bucket air-quality-eda nếu chưa có
        eda_bucket = "air-quality-eda"
        if not minio_client.bucket_exists(eda_bucket):
            minio_client.make_bucket(eda_bucket)
            st.success(f"Đã tạo bucket '{eda_bucket}'")
        
        # Xác định năm dữ liệu
        years = df['year'].unique()
        if len(years) == 1:
            year_str = str(years[0])
            folder_name = f"tuong_quan_{year_str}"
            file_name = f"{folder_name}/ma_tran_tuong_quan_{year_str}_latest.csv"
        else:
            year_str = f"{df['year'].min()}_{df['year'].max()}"
            folder_name = f"tuong_quan_{year_str}"
            file_name = f"{folder_name}/ma_tran_tuong_quan_{year_str}_latest.csv"
        
        # Chuẩn bị file CSV
        csv_data = overall_corr.to_csv()
        csv_bytes = BytesIO(csv_data.encode('utf-8'))
        
        # Upload lên MinIO (sẽ tự động ghi đè nếu file đã tồn tại)
        minio_client.put_object(
            bucket_name=eda_bucket,
            object_name=file_name,
            data=csv_bytes,
            length=len(csv_data.encode('utf-8')),
            content_type='text/csv'
        )
        
        current_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        st.success(f"Đã lưu ma trận tương quan lên MinIO: `{eda_bucket}/{file_name}` (Lúc {current_time})")
       
        
    except Exception as e:
        st.error(f"Lỗi khi lưu lên MinIO: {str(e)}")
        st.warning("Vui lòng kiểm tra kết nối MinIO và quyền truy cập")
        
    # Phân tích chi tiết
    st.subheader("Phân Tích Chi Tiết Tương Quan")
    
    # Tìm các cặp tương quan mạnh nhất
    strong_correlations = df_corr[abs(df_corr['correlation']) > 0.7] if not df_corr.empty else pd.DataFrame()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Top 10 Tương Quan Mạnh Nhất (>0.7):**")
        if not strong_correlations.empty:
            top_correlations = (
                strong_correlations
                .groupby(['pollutant_1', 'pollutant_2'])['correlation']
                .mean()
                .reset_index()
                .sort_values('correlation', key=abs, ascending=False)
                .head(10)
            )
            
            fig_top_corr = px.bar(
                top_correlations,
                x='correlation',
                y=[f"{row['pollutant_1']} - {row['pollutant_2']}" for _, row in top_correlations.iterrows()],
                orientation='h',
                color='correlation',
                color_continuous_scale='RdBu',
                title="Top 10 Tương Quan Mạnh Nhất"
            )
            st.plotly_chart(fig_top_corr, use_container_width=True)
        else:
            st.info("Không tìm thấy tương quan mạnh (>0.7)")
    
    with col2:
        st.write("**Tương Quan Theo Địa Điểm:**")
        
        available_pollutants_for_selection = [p for p in pollutants if p in df.columns]
        if len(available_pollutants_for_selection) >= 2:
            selected_pollutant_pair = st.selectbox(
                "Chọn cặp chất để xem tương quan theo địa điểm:",
                options=[f"{p1} - {p2}" for i, p1 in enumerate(available_pollutants_for_selection) for p2 in available_pollutants_for_selection[i+1:]],
                index=0,
                key="correlation_pair_select"
            )
        else:
            selected_pollutant_pair = None
        
        if selected_pollutant_pair and not df_corr.empty:
            p1, p2 = selected_pollutant_pair.split(' - ')
            
            location_corr = (
                df_corr[
                    (df_corr['pollutant_1'] == p1) & 
                    (df_corr['pollutant_2'] == p2)
                ]
                .groupby('location_key')['correlation']
                .mean()
                .reset_index()
            )
            
            if not location_corr.empty:
                fig_loc_corr = px.bar(
                    location_corr,
                    x='location_key',
                    y='correlation',
                    color='correlation',
                    color_continuous_scale='RdBu',
                    title=f"Tương Quan {selected_pollutant_pair} Theo Địa Điểm"
                )
                st.plotly_chart(fig_loc_corr, use_container_width=True)
            else:
                st.info("Không có dữ liệu tương quan cho cặp này")
        elif not selected_pollutant_pair:
            st.info("Không đủ chất ô nhiễm để phân tích")
    
    
    
    return df_corr

def main():
    """Hàm chính của ứng dụng"""
    st.title("Phân Tích Chất Lượng Không Khí Chuyên Sâu")
    st.markdown("---")
    
    # Sidebar cho navigation và chọn file
    st.sidebar.title("Menu Phân Tích")
    
    # Phần chọn file dữ liệu
    st.sidebar.subheader(" Chọn Dữ Liệu")
    
    # Lấy danh sách file từ MinIO
    try:
        client = get_minio_client()
        available_files = list_combined_files(client, MINIO_CLEAN_BUCKET)
        
        if available_files:
            # Tạo dictionary để hiển thị tên file ngắn gọn hơn
            file_display_names = {}
            for file in available_files:
                # Lấy tên file từ đường dẫn
                filename = file.split('/')[-1]
                # Loại bỏ extension và format đẹp hơn
                display_name = filename.replace('.csv', '').replace('openmeteo_', '').replace('_', ' ').title()
                file_display_names[display_name] = file
            
            st.sidebar.info(f"Tìm thấy {len(available_files)} file dữ liệu")
            
            # Selectbox để chọn file
            selected_display_name = st.sidebar.selectbox(
                "Chọn file dữ liệu:",
                options=list(file_display_names.keys()),
                help="Chọn file dữ liệu đã được làm sạch từ MinIO"
            )
            
            selected_file = file_display_names[selected_display_name]
            
            # Hiển thị thông tin file được chọn
            st.sidebar.success(f"File: `{selected_file.split('/')[-1]}`")
            
        else:
            st.sidebar.error("Không tìm thấy file dữ liệu")
            st.sidebar.info("Vui lòng chạy quy trình làm sạch dữ liệu trước")
            selected_file = None
            
    except Exception as e:
        st.sidebar.error(f"Lỗi kết nối MinIO: {str(e)[:100]}")
        selected_file = None
    
    # Menu phân tích
    st.sidebar.subheader("Loại Phân Tích")
    analysis_option = st.sidebar.selectbox(
        "Chọn phần phân tích mà bạn muốn xem:",
        [
            "Tổng Quan",
            "Phân Vùng & Xếp Hạng",
            "Chất Ô Nhiễm Chính",
            "Phân Tích Mùa Vụ",
            "Ma Trận Tương Quan & Mối Quan Hệ",
            "Tất Cả Phân Tích"
        ]
    )
    
    # Load dữ liệu với file được chọn
    if selected_file:
        with st.spinner("Đang tải dữ liệu từ MinIO..."):
            df = load_data(selected_file)
    else:
        df = None
    
    if df is None:
        st.error("Không thể tải dữ liệu từ MinIO")
        st.markdown("""
        ### Hướng dẫn khắc phục:
        1. **Kiểm tra MinIO server**: Đảm bảo MinIO đang chạy tại `172.27.91.163:9004`
        2. **Kiểm tra bucket**: Bucket `air-quality-clean` phải tồn tại
        3. **Kiểm tra dữ liệu**: Phải có file CSV trong `openmeteo/global/`
        4. **Chạy tiền xử lý**: Hãy chạy quy trình làm sạch dữ liệu trước
        """)
        return
    
    # Hiển thị thông tin tổng quan
    if analysis_option == "Tổng Quan":
        st.header("Tổng Quan Chất Lượng Không Khí")
        
        # Metrics chính
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Tổng số bản ghi", f"{len(df):,}")
        with col2:
            st.metric("Số địa điểm", df['location_key'].nunique())
        with col3:
            year_min = df['year'].min()
            year_max = df['year'].max()
            time_range = str(year_min) if year_min == year_max else f"{year_min} - {year_max}"
            st.metric("Khoảng thời gian", time_range)
        with col4:
            avg_aqi = df['aqi'].mean()
            if avg_aqi <= 50:
                aqi_status = "Tốt"
            elif avg_aqi <= 100:
                aqi_status = "Trung Bình"
            elif avg_aqi <= 150:
                aqi_status = "Không Tốt Nhóm Nhạy Cảm"
            elif avg_aqi <= 200:
                aqi_status = "Không Tốt"
            elif avg_aqi <= 300:
                aqi_status = "Rất Không Tốt"
            else:
                aqi_status = "Nguy Hiểm"
            st.metric("AQI trung bình", f"{avg_aqi:.1f}", delta=aqi_status)
        
        # Phân tích chất lượng không khí tổng thể
        st.subheader("Tình Trạng Chất Lượng Không Khí")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Phân bố AQI theo mức độ
            def classify_aqi(aqi):
                if aqi <= 50: return "🟢 Tốt"
                elif aqi <= 100: return "🟡 Trung Bình"
                elif aqi <= 150: return "🟠 Không Tốt Cho Nhóm Nhạy Cảm"
                elif aqi <= 200: return "🔴 Không Tốt"
                elif aqi <= 300: return "🟣 Rất Không Tốt"
                else: return "⚫ Nguy Hiểm"
            
            df['aqi_level'] = df['aqi'].apply(classify_aqi)
            aqi_distribution = df['aqi_level'].value_counts()
            
            fig_aqi_dist = px.pie(
                values=aqi_distribution.values,
                names=aqi_distribution.index,
                title="Phân Bố Mức Độ AQI",
                color_discrete_sequence=['#009966', '#ffde33', '#ff9933', '#cc0033', '#660099', '#7e0023']
            )
            st.plotly_chart(fig_aqi_dist, use_container_width=True)
        
        with col2:
            # Phân bố AQI theo các mức ngưỡng WHO
            aqi_thresholds = {
                'Tốt (0-50)': len(df[df['aqi'] <= 50]),
                'Trung Bình (51-100)': len(df[(df['aqi'] > 50) & (df['aqi'] <= 100)]),
                'Không Tốt Nhóm Nhạy Cảm (101-150)': len(df[(df['aqi'] > 100) & (df['aqi'] <= 150)]),
                'Không Tốt (151-200)': len(df[(df['aqi'] > 150) & (df['aqi'] <= 200)]),
                'Rất Không Tốt (201-300)': len(df[(df['aqi'] > 200) & (df['aqi'] <= 300)]),
                'Nguy Hiểm (>300)': len(df[df['aqi'] > 300])
            }
            
            fig_threshold = px.bar(
                x=list(aqi_thresholds.keys()),
                y=list(aqi_thresholds.values()),
                title="Phân Bố Số Ngày Theo Ngưỡng AQI",
                color=list(aqi_thresholds.values()),
                color_continuous_scale='RdYlGn_r'
            )
            fig_threshold.update_layout(
                xaxis_title="Mức Độ AQI",
                yaxis_title="Số Ngày",
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig_threshold, use_container_width=True)
        
        # Thống kê các chất ô nhiễm chính
        st.subheader("Thống Kê Các Chất Ô Nhiễm")
        
        pollutants = ['pm25', 'pm10', 'no2', 'so2', 'co', 'o3']
        available_pollutants = [p for p in pollutants if p in df.columns]
        
        if available_pollutants:
            col1, col2, col3 = st.columns(3)
            
            for i, pollutant in enumerate(available_pollutants[:3]):
                with [col1, col2, col3][i]:
                    avg_val = df[pollutant].mean()
                    max_val = df[pollutant].max()
                    
                    # Đánh giá mức độ
                    if pollutant in ['pm25', 'pm10']:
                        status = "An toàn" if avg_val <= 15 else "Cảnh báo" if avg_val <= 35 else "Nguy hiểm"
                    else:
                        status = "Bình thường" if avg_val <= 40 else "Cao"
                    
                    st.metric(
                        f"{pollutant.upper()} TB (μg/m³)",
                        f"{avg_val:.1f}",
                        delta=status
                    )
            
            # Biểu đồ so sánh các chất ô nhiễm
            pollutant_stats = df[available_pollutants].mean()
            
            fig_pollutants = px.bar(
                x=pollutant_stats.index,
                y=pollutant_stats.values,
                title="Nồng Độ Trung Bình Các Chất Ô Nhiễm",
                color=pollutant_stats.values,
                color_continuous_scale='Plasma'
            )
            fig_pollutants.update_layout(
                xaxis_title="Chất Ô Nhiễm",
                yaxis_title="Nồng Độ Trung Bình (μg/m³)"
            )
            st.plotly_chart(fig_pollutants, use_container_width=True)
        
        # Xu hướng theo thời gian
        st.subheader("Xu Hướng Theo Thời Gian")
        
        # AQI theo tháng
        monthly_aqi = df.groupby(['year', 'month'])['aqi'].mean().reset_index()
        monthly_aqi['date'] = pd.to_datetime(monthly_aqi[['year', 'month']].assign(day=1))
        
        fig_trend = px.line(
            monthly_aqi,
            x='date',
            y='aqi',
            title="Xu Hướng AQI Theo Thời Gian",
            markers=True
        )
        fig_trend.update_layout(
            xaxis_title="Thời Gian",
            yaxis_title="AQI Trung Bình"
        )
        st.plotly_chart(fig_trend, use_container_width=True)
        
        # Thống kê chi tiết toàn diện
        st.subheader("Thống Kê Chi Tiết Toàn Diện")
        
        # Tạo 3 cột cho layout tốt hơn
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Chỉ Số Chất Lượng Không Khí:**")
            
            # Tính toán các percentile
            aqi_p25 = df['aqi'].quantile(0.25)
            aqi_p75 = df['aqi'].quantile(0.75)
            aqi_median = df['aqi'].median()
            
            summary_stats = {
                "AQI trung bình": f"{df['aqi'].mean():.1f}",
                "AQI trung vị": f"{aqi_median:.1f}",
                "AQI cao nhất": f"{df['aqi'].max():.1f}",
                "AQI thấp nhất": f"{df['aqi'].min():.1f}",
                "Percentile 25%": f"{aqi_p25:.1f}",
                "Percentile 75%": f"{aqi_p75:.1f}",
                "Độ lệch chuẩn": f"{df['aqi'].std():.1f}",
                "Hệ số biến thiên": f"{(df['aqi'].std()/df['aqi'].mean()*100):.1f}%"
            }
            
            for key, value in summary_stats.items():
                st.write(f"- **{key}**: {value}")
        
        with col2:
            st.write("**Phân Tích Ngưỡng Nguy Hiểm:**")
            
            # Tính toán các ngưỡng nguy hiểm
            danger_stats = {
                "% ngày AQI > 50 (Trung bình)": f"{(df['aqi'] > 50).mean()*100:.2f}%",
                "% ngày AQI > 100 (Không Tốt Cho Nhóm Nhạy Cảm)": f"{(df['aqi'] > 100).mean()*100:.2f}%",
                "% ngày AQI > 150 (Không Tốt)": f"{(df['aqi'] > 150).mean()*100:.2f}%",
                "% ngày AQI > 200 (Rất Không Tốt)": f"{(df['aqi'] > 200).mean()*100:.2f}%",
                "% ngày AQI > 300 (Nguy Hiểm)": f"{(df['aqi'] > 300).mean()*100:.2f}%",
                "Số ngày tốt (AQI ≤ 50)": f"{(df['aqi'] <= 50).sum():,}",
                "Số ngày kém (AQI > 100)": f"{(df['aqi'] > 100).sum():,}",
                "Ngày ô nhiễm nhất": f"AQI {df['aqi'].max():.0f}",
                "Tần suất AQI > 100": f"1/{int(1/((df['aqi'] > 100).mean() + 0.0001))}"
            }
            
            for key, value in danger_stats.items():
                st.write(f"- **{key}**: {value}")
        
        with col3:
            st.write("**Thông Tin Cấu Trúc Dữ Liệu:**")
            
            # Phân tích cấu trúc dữ liệu
            data_completeness = {}
            key_columns = ['aqi', 'pm25', 'pm10', 'no2', 'so2', 'co', 'o3']
            
            for col in key_columns:
                if col in df.columns:
                    completeness = (df[col].notna().sum() / len(df)) * 100
                    data_completeness[f"Đầy đủ {col.upper()}"] = f"{completeness:.1f}%"
            
            # Thông tin tổng quan
            year_min = df['year'].min()
            year_max = df['year'].max()
            year_range_display = str(year_min) if year_min == year_max else f"{year_min}-{year_max}"
            
            general_info = {
                "Tổng số bản ghi": f"{len(df):,}",
                "Số địa điểm": f"{df['location_key'].nunique()}",
                "Khoảng thời gian": year_range_display,
                "Số năm dữ liệu": f"{df['year'].nunique()}",
                "Số tháng có dữ liệu": f"{df['month'].nunique()}/12",
                "Trung bình bản ghi/địa điểm": f"{len(df)/df['location_key'].nunique():.0f}"
            }
            
            # Hiển thị thông tin chung
            for key, value in general_info.items():
                st.write(f"- **{key}**: {value}")
            
            st.write("\n**Độ Đầy Đủ Dữ Liệu:**")
            for key, value in data_completeness.items():
                st.write(f"- **{key}**: {value}")
        
        # Bảng thống kê mô tả chi tiết
        st.subheader("Bảng Thống Kê Mô Tả Các Chỉ Số")
        
        # Chọn các cột quan trọng để hiển thị
        important_cols = ['aqi', 'pm25', 'pm10', 'no2', 'so2', 'co', 'o3']
        available_cols = [col for col in important_cols if col in df.columns]
        
        if available_cols:
            desc_stats = df[available_cols].describe().round(2)
            
            # Hiển thị bảng mô tả cơ bản
            st.write("**Thống Kê Các Chỉ Số Chất Lượng Không Khí:**")
            st.dataframe(desc_stats, use_container_width=True)
            
            # Giải thích các chỉ số
            with st.expander("Giải Thích Các Chỉ Số Thống Kê"):
                st.markdown("""
                **Ý nghĩa các chỉ số trong bảng:**
                - **count**: Số lượng ngày có dữ liệu đo được
                - **mean**: Nồng độ trung bình của chất ô nhiễm
                - **std**: Độ biến động của chất ô nhiễm (càng cao càng không ổn định)
                - **min/max**: Nồng độ thấp nhất/cao nhất ghi nhận được
                - **25%**: 25% ngày có nồng độ thấp hơn giá trị này
                - **50%**: Giá trị trung vị (50% ngày thấp hơn, 50% cao hơn)
                - **75%**: 75% ngày có nồng độ thấp hơn giá trị này
                
                **Đơn vị đo:**
                - **AQI**: Chỉ số chất lượng không khí (0-500, càng thấp càng tốt)
                - **PM2.5, PM10, NO2, SO2, CO, O3**: μg/m³ (microgram/mét khối)
                """)
    
    # Chạy các bài phân tích
    elif analysis_option == "Phân Vùng & Xếp Hạng":
        phan_vung_o_nhiem(df)
    
    elif analysis_option == "Chất Ô Nhiễm Chính":
        chat_o_nhiem(df)
    
    elif analysis_option == "Phân Tích Mùa Vụ":
        phan_tich_mua_vu(df)
    
    elif analysis_option == "Ma Trận Tương Quan & Mối Quan Hệ":
        ma_tran_tuong_quan(df)
    
    elif analysis_option == "Tất Cả Phân Tích":
        st.info("Đang chạy tất cả các phân tích - có thể mất vài phút...")
        
        df_rank = phan_vung_o_nhiem(df)
        st.markdown("---")
        
        df_pollutant = chat_o_nhiem(df)
        st.markdown("---")
        
        df_seasonal = phan_tich_mua_vu(df)
        st.markdown("---")
        
        df_corr = ma_tran_tuong_quan(df)
        st.markdown("---")
        
        st.success("✅ Hoàn thành tất cả phân tích!")

if __name__ == "__main__":
    main()