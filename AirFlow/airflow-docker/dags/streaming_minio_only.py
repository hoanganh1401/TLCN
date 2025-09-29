"""
Air Quality Streaming to MinIO (No PostgreSQL, Streaming-only)
================================================================

This DAG periodically fetches air quality data (WAQI) and stores RAW JSON
objects into MinIO under a partitioned path in the Raw Zone.

Scope per user request:
- Only streaming (no historical backfill)
- Store to MinIO only (remove PostgreSQL usage)

Zones (current implementation writes to Raw Zone only):
- Raw Zone: JSON
- Processed Zone: CSV (planned)
- Analytics Zone: Parquet (planned)
"""

from datetime import datetime, timedelta
import json
import io
import os
import time
import logging

import requests
from minio import Minio

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
    AIRFLOW_AVAILABLE = True
except Exception:
    AIRFLOW_AVAILABLE = False


# Configuration from environment
MINIO_HOST = os.environ.get('MINIO_HOST', 'minio:9000')
MINIO_ACCESS = os.environ.get('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET = os.environ.get('MINIO_SECRET_KEY', 'admin123')
BUCKET = os.environ.get('MINIO_BUCKET', 'air-quality')

# Accept both WAQI_TOKEN and WAQI_API_KEY (compose sets WAQI_API_KEY)
WAQI_TOKEN = os.environ.get('WAQI_TOKEN') or os.environ.get('WAQI_API_KEY', '')
BASE_URL = 'https://api.waqi.info'

# Streaming interval via schedule_interval (default: every 5 minutes)


# Cities list as requested: (station_id, city_name)
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


def _get_minio_client():
    return Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)


def fetch_waqi_feed(city_name: str):
    """Fetch WAQI feed for a city.

    Strategy:
    1. Try the simple /feed/{city_name}/ call (keeps existing behavior).
    2. If that fails or returns no data, use the /search/{city_name}/ endpoint to
       find a station UID and then call /feed/@{uid}/.

    This makes lookups more robust when the city name isn't the exact WAQI slug.
    """
    # 1) Try direct feed by name (existing behavior)
    try:
        url = f"{BASE_URL}/feed/{city_name}/"
        r = requests.get(url, params={'token': WAQI_TOKEN}, timeout=15)
        r.raise_for_status()
        payload = r.json()
        if payload and payload.get('status') == 'ok':
            return payload
    except Exception:
        # fallthrough to search-based resolution
        pass

    # 2) Fallback: use search API to resolve to a station uid and request by uid
    try:
        search_url = f"{BASE_URL}/search/{city_name}/"
        rs = requests.get(search_url, params={'token': WAQI_TOKEN}, timeout=15)
        rs.raise_for_status()
        sdata = rs.json()
        if sdata and sdata.get('status') == 'ok' and sdata.get('data'):
            first = sdata['data'][0]
            uid = first.get('uid') or first.get('station', {}).get('uid')
            # If we have coordinates instead, try geo lookup
            geo = None
            station = first.get('station') if isinstance(first.get('station'), dict) else None
            if station:
                geo = station.get('geo')
            if uid:
                try:
                    feed_url = f"{BASE_URL}/feed/@{uid}/"
                    rf = requests.get(feed_url, params={'token': WAQI_TOKEN}, timeout=15)
                    rf.raise_for_status()
                    return rf.json()
                except Exception:
                    pass
            if geo and isinstance(geo, (list, tuple)) and len(geo) >= 2:
                lat, lon = geo[0], geo[1]
                try:
                    feed_geo_url = f"{BASE_URL}/feed/geo:{lat};{lon}/"
                    rg = requests.get(feed_geo_url, params={'token': WAQI_TOKEN}, timeout=15)
                    rg.raise_for_status()
                    return rg.json()
                except Exception:
                    pass
    except Exception:
        # final fallback: return whatever the initial attempt produced (likely error)
        pass

    # If all attempts fail, return a simple error-shaped dict so caller can log it
    return {'status': 'error', 'data': None, 'message': f'Could not resolve feed for {city_name}'}


def save_raw_to_minio(raw_payload, city_label, measured_at=None, logger: logging.Logger = None):
    client = _get_minio_client()
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    timestamp = (
        measured_at.isoformat() if hasattr(measured_at, 'isoformat') and measured_at is not None
        else datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    )
    safe_city = city_label.replace(' ', '_').replace('-', '_')
    object_name = (
        f"raw/waqi/city={safe_city}/year={timestamp[:4]}/month={timestamp[5:7]}/"
        f"day={timestamp[8:10]}/{timestamp}.json"
    )
    data = json.dumps(raw_payload).encode('utf-8')
    client.put_object(BUCKET, object_name, io.BytesIO(data), length=len(data), content_type='application/json')
    if logger:
        logger.info(f"Saved RAW JSON to MinIO: s3://{BUCKET}/{object_name}")


def collect_and_store_raw():
    logger = logging.getLogger('airflow.task')

    if not WAQI_TOKEN:
        logger.warning("WAQI token is not set; skipping fetch")
        return

    # quick MinIO readiness check
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

    fetched = 0
    errors = 0

    for station_id, city_name in CITIES:
        try:
            payload = fetch_waqi_feed(city_name)
            if not payload or payload.get('status') != 'ok':
                errors += 1
                logger.warning(f"No WAQI data for {station_id}")
                continue

            measured_iso = payload.get('data', {}).get('time', {}).get('iso')
            measured_at = None
            try:
                if measured_iso:
                    measured_at = datetime.fromisoformat(measured_iso.replace('Z', '+00:00'))
            except Exception:
                measured_at = None

            save_raw_to_minio(payload, station_id, measured_at=measured_at, logger=logger)
            fetched += 1
            time.sleep(0.5)
        except Exception as e:
            errors += 1
            logger.error(f"Error for {station_id}: {e}")

    logger.info(f"Round finished: fetched={fetched}, errors={errors}")


if AIRFLOW_AVAILABLE:
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }

    dag = DAG(
        'air_quality_streaming_minio_only',
        default_args=default_args,
        description='Stream WAQI data and store RAW JSON to MinIO only',
        schedule_interval=timedelta(minutes=5),
        catchup=False,
        tags=['air-quality', 'streaming', 'minio', 'raw-zone'],
    )

    stream_to_minio_task = PythonOperator(
        task_id='collect_and_store_raw',
        python_callable=collect_and_store_raw,
        dag=dag,
    )


# Allow running this file directly for a single streaming round (outside Airflow)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    collect_and_store_raw()


