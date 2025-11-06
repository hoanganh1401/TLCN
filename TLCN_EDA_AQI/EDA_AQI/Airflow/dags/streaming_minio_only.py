from datetime import datetime, timedelta
import json
import io
import os
import time
import logging
import requests
from minio import Minio
import pandas as pd

# -------------------------------------------------------------
# Airflow imports (luôn import trực tiếp, không dùng try/except)
# -------------------------------------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------
MINIO_HOST = os.environ.get('MINIO_HOST', 'minio:9000')
MINIO_ACCESS = os.environ.get('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET = os.environ.get('MINIO_SECRET_KEY', 'admin123')
BUCKET = os.environ.get('MINIO_BUCKET', 'air-quality')

WAQI_TOKEN = os.environ.get('WAQI_TOKEN') or os.environ.get('WAQI_API_KEY', '4ab752a4-5aaf-48dc-8130-837c88e9efbe')
BASE_URL = 'https://api.waqi.info'

CITIES = [
    ("Hanoi", "Hanoi"),
    ("Ho Chi Minh City", "Ho Chi Minh"),
    ("Da Nang", "Da Nang"),
    ("Can Tho", "Can Tho"),
    ("Hai Phong", "Hai Phong"),
    ("An Giang", "An Giang"),
    ("Ba Ria-Vung Tau", "Ba Ria-Vung Tau"),
    ("Bac Giang", "Bac Giang"),
    ("Bac Kan", "Bac Kan"),
    ("Bac Lieu", "Bac Lieu"),
    ("Bac Ninh", "Bac Ninh"),
    ("Ben Tre", "Ben Tre"),
    ("Binh Dinh", "Binh Dinh"),
    ("Binh Duong", "Binh Duong"),
    ("Binh Phuoc", "Binh Phuoc"),
    ("Binh Thuan", "Binh Thuan"),
    ("Ca Mau", "Ca Mau"),
    ("Cao Bang", "Cao Bang"),
    ("Dak Lak", "Dak Lak"),
    ("Dak Nong", "Dak Nong"),
    ("Dien Bien", "Dien Bien"),
    ("Dong Nai", "Dong Nai"),
    ("Dong Thap", "Dong Thap"),
    ("Gia Lai", "Gia Lai"),
    ("Ha Giang", "Ha Giang"),
    ("Ha Nam", "Ha Nam"),
    ("Ha Tinh", "Ha Tinh"),
    ("Hai Duong", "Hai Duong"),
    ("Hau Giang", "Hau Giang"),
    ("Hoa Binh", "Hoa Binh"),
    ("Hung Yen", "Hung Yen"),
    ("Khanh Hoa", "Khanh Hoa"),
    ("Kien Giang", "Kien Giang"),
    ("Kon Tum", "Kon Tum"),
    ("Lai Chau", "Lai Chau"),
    ("Lam Dong", "Lam Dong"),
    ("Lang Son", "Lang Son"),
    ("Lao Cai", "Lao Cai"),
    ("Long An", "Long An"),
    ("Nam Dinh", "Nam Dinh"),
    ("Nghe An", "Nghe An"),
    ("Ninh Binh", "Ninh Binh"),
    ("Ninh Thuan", "Ninh Thuan"),
    ("Phu Tho", "Phu Tho"),
    ("Phu Yen", "Phu Yen"),
    ("Quang Binh", "Quang Binh"),
    ("Quang Nam", "Quang Nam"),
    ("Quang Ngai", "Quang Ngai"),
    ("Quang Ninh", "Quang Ninh"),
    ("Quang Tri", "Quang Tri"),
    ("Soc Trang", "Soc Trang"),
    ("Son La", "Son La"),
    ("Tay Ninh", "Tay Ninh"),
    ("Thai Binh", "Thai Binh"),
    ("Thai Nguyen", "Thai Nguyen"),
    ("Thanh Hoa", "Thanh Hoa"),
    ("Thua Thien-Hue", "Thua Thien-Hue"),
    ("Tien Giang", "Tien Giang"),
    ("Tra Vinh", "Tra Vinh"),
    ("Tuyen Quang", "Tuyen Quang"),
    ("Vinh Long", "Vinh Long"),
    ("Vinh Phuc", "Vinh Phuc"),
    ("Yen Bai", "Yen Bai"),
]

# -------------------------------------------------------------
# Helper
# -------------------------------------------------------------
def _get_minio_client():
    """Create and return a MinIO client instance."""
    return Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

def _safe_city_label(city_label: str) -> str:
    """Normalize city label for object naming."""
    return city_label.replace(' ', '_').replace('-', '_')

# -------------------------------------------------------------
# Extract → RAW ZONE
# -------------------------------------------------------------
def fetch_waqi_feed(city_name: str):
    """Fetch air quality data from WAQI API for a city."""
    url = f"{BASE_URL}/feed/{city_name}/"
    r = requests.get(url, params={'token': WAQI_TOKEN}, timeout=15)
    r.raise_for_status()
    return r.json()

def save_raw_to_minio(raw_payload, city_label, measured_at=None, logger: logging.Logger = None):
    """Save raw WAQI JSON response to MinIO."""
    client = _get_minio_client()
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    timestamp = (
        measured_at.isoformat() if hasattr(measured_at, 'isoformat') and measured_at is not None
        else datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
    )
    safe_city = _safe_city_label(city_label)
    object_name = (
        f"waqi_raw/city={safe_city}/year={timestamp[:4]}/month={timestamp[5:7]}/"
        f"day={timestamp[8:10]}/data_{timestamp}.json"
    )

    data = json.dumps(raw_payload).encode('utf-8')
    client.put_object(BUCKET, object_name, io.BytesIO(data), length=len(data), content_type='application/json')

    if logger:
        logger.info(f"Saved RAW JSON to MinIO: s3://{BUCKET}/{object_name}")

def collect_and_store_raw():
    """Extract task: Fetch WAQI data for each city and store to MinIO (raw zone)."""
    logger = logging.getLogger('airflow.task')

    if not WAQI_TOKEN:
        logger.warning("WAQI token is not set; skipping fetch")
        return

    # Check MinIO connection
    retries = 6
    for i in range(retries):
        try:
            client = _get_minio_client()
            client.list_buckets()
            break
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(5)

    for station_id, city_name in CITIES:
        try:
            payload = fetch_waqi_feed(city_name)
            if not payload or payload.get('status') != 'ok':
                logger.warning(f"No WAQI data for {station_id}")
                continue

            measured_iso = payload.get('data', {}).get('time', {}).get('iso')
            measured_at = None
            if measured_iso:
                try:
                    measured_at = datetime.fromisoformat(measured_iso.replace('Z', '+00:00'))
                except Exception:
                    measured_at = None

            save_raw_to_minio(payload, station_id, measured_at=measured_at, logger=logger)
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Error for {station_id}: {e}")

# -------------------------------------------------------------
# Transform → PROCESSED ZONE
# -------------------------------------------------------------
def list_raw_objects_for_city(client: Minio, city_label: str):
    """List all raw data objects in MinIO for a given city."""
    safe = _safe_city_label(city_label)
    prefix = f"waqi_raw/city={safe}/"
    return [o.object_name for o in client.list_objects(BUCKET, prefix=prefix, recursive=True)]

def _load_json_from_minio(client: Minio, object_name: str):
    """Load JSON file from MinIO."""
    obj = client.get_object(BUCKET, object_name)
    data = obj.read()
    obj.close()
    obj.release_conn()
    return json.loads(data.decode('utf-8'))

def transform_city_to_csv(city_label: str, logger: logging.Logger = None) -> int:
    """Transform raw JSON to CSV and save to MinIO (processed zone)."""
    client = _get_minio_client()
    objs = list_raw_objects_for_city(client, city_label)
    if not objs:
        return 0

    records = []
    for obj_name in objs:
        try:
            payload = _load_json_from_minio(client, obj_name)
            d = payload.get('data', {}) or {}
            time_iso = d.get('time', {}).get('iso')
            rec = {'city': city_label, 'measured_at': time_iso, 'aqi': d.get('aqi')}
            iaqi = d.get('iaqi', {}) or {}
            for k, v in iaqi.items():
                rec[f'iaqi_{k}'] = v['v'] if isinstance(v, dict) and 'v' in v else v
            records.append(rec)
        except Exception as e:
            if logger:
                logger.warning(f"Skipping {obj_name}: {e}")

    if not records:
        return 0

    df = pd.DataFrame.from_records(records)
    df['measured_at'] = pd.to_datetime(df['measured_at'], utc=True, errors='coerce')
    df['year'] = df['measured_at'].dt.strftime('%Y')
    df['month'] = df['measured_at'].dt.strftime('%m')
    df['day'] = df['measured_at'].dt.strftime('%d')

    total_rows = 0
    for (y, m, d), partition_df in df.groupby(['year', 'month', 'day']):
        safe = _safe_city_label(city_label)
        path = f"waqi_processed/city={safe}/year={y}/month={m}/day={d}/data_cleaned.csv"
        csv_bytes = partition_df.to_csv(index=False).encode('utf-8')
        client.put_object(BUCKET, path, io.BytesIO(csv_bytes), length=len(csv_bytes), content_type='text/csv')
        total_rows += len(partition_df)
        if logger:
            logger.info(f"Wrote {len(partition_df)} rows to s3://{BUCKET}/{path}")
    return total_rows

def transform_all(logger: logging.Logger = None):
    """Transform all cities from raw to processed zone."""
    total = 0
    for station_id, _ in CITIES:
        total += transform_city_to_csv(station_id, logger=logger)
    return total

# -------------------------------------------------------------
# Load → ANALYTICS ZONE
# -------------------------------------------------------------
def analytics_city_to_parquet(city_label: str, logger: logging.Logger = None) -> int:
    """Convert processed CSVs to Parquet (analytics zone)."""
    client = _get_minio_client()
    safe = _safe_city_label(city_label)
    prefix = f"waqi_processed/city={safe}/"
    objs = [o.object_name for o in client.list_objects(BUCKET, prefix=prefix, recursive=True)]
    if not objs:
        return 0

    total_rows = 0
    for obj_name in objs:
        if not obj_name.endswith(".csv"):
            continue
        try:
            obj = client.get_object(BUCKET, obj_name)
            df = pd.read_csv(io.BytesIO(obj.read()))
            obj.close()
            obj.release_conn()

            df['measured_at'] = pd.to_datetime(df['measured_at'], utc=True, errors='coerce')
            df['year'] = df['measured_at'].dt.strftime('%Y')
            df['month'] = df['measured_at'].dt.strftime('%m')
            df['day'] = df['measured_at'].dt.strftime('%d')

            for (y, m, d), part_df in df.groupby(['year', 'month', 'day']):
                path = f"waqi_analytics/city={safe}/year={y}/month={m}/day={d}/data_final.parquet"
                parquet_bytes = part_df.to_parquet(index=False, engine="pyarrow")
                client.put_object(
                    BUCKET, path, io.BytesIO(parquet_bytes),
                    length=len(parquet_bytes), content_type='application/octet-stream'
                )
                total_rows += len(part_df)
                if logger:
                    logger.info(f"Wrote {len(part_df)} rows to s3://{BUCKET}/{path}")
        except Exception as e:
            if logger:
                logger.warning(f"Analytics transform failed for {obj_name}: {e}")
    return total_rows

def analytics_all(logger: logging.Logger = None):
    """Transform all cities from processed to analytics zone."""
    total = 0
    for station_id, _ in CITIES:
        total += analytics_city_to_parquet(station_id, logger=logger)
    return total

# -------------------------------------------------------------
# Airflow DAG definition
# -------------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='air_quality_etl_minio',
    default_args=default_args,
    description='ETL WAQI data with RAW/PROCESSED/ANALYTICS zones in MinIO',
    schedule=timedelta(minutes=1),  # ← dùng 'schedule' thay vì 'schedule_interval'
    catchup=False,
    tags=['air-quality', 'etl', 'minio'],
)


extract_task = PythonOperator(
    task_id='collect_and_store_raw',
    python_callable=collect_and_store_raw,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_all_to_csv',
    python_callable=transform_all,
    dag=dag,
)

analytics_task = PythonOperator(
    task_id='analytics_all_to_parquet',
    python_callable=analytics_all,
    dag=dag,
)

extract_task >> transform_task >> analytics_task

# -------------------------------------------------------------
# Debug mode (run manually)
# -------------------------------------------------------------
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    collect_and_store_raw()
    transform_all(logging.getLogger())
    analytics_all(logging.getLogger())
