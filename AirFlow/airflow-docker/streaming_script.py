from datetime import datetime, timedelta
import json
import io
import os
import requests
from urllib.parse import urlparse
import time
import threading
import logging

from minio import Minio
import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/streaming.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration from environment
MINIO_HOST = os.environ.get('MINIO_HOST', 'minio:9000')
MINIO_ACCESS = os.environ.get('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET = os.environ.get('MINIO_SECRET_KEY', 'admin123')
BUCKET = os.environ.get('MINIO_BUCKET', 'air-quality')

POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'airflow')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
POSTGRES_PORT = int(os.environ.get('POSTGRES_PORT', 5432))

# APIs
WAQI_TOKEN = os.environ.get('WAQI_TOKEN', '73f88f4b0cf15fdc4984cc221d78cff444761f42')
BASE_URL = 'https://api.waqi.info'
OPEN_METEO_BASE_URL = 'https://air-quality-api.open-meteo.com/v1/air-quality'

# Streaming interval (seconds)
STREAMING_INTERVAL = int(os.environ.get('STREAMING_INTERVAL', 300))  # 5 minutes default

# Vietnam stations (cleaned up - no duplicates)
CITIES_WITH_COORDS = [
    # Major cities with multiple stations
    {"name": "Hanoi", "station": "Hoan Kiem", "lat": 21.0285, "lon": 105.8542},
    {"name": "Hanoi", "station": "Cau Giay", "lat": 21.0328, "lon": 105.7938},
    {"name": "Hanoi", "station": "Dong Da", "lat": 21.0245, "lon": 105.8412},
    {"name": "Hanoi", "station": "Ba Dinh", "lat": 21.0355, "lon": 105.8320},
    
    {"name": "Ho Chi Minh City", "station": "District 1", "lat": 10.8231, "lon": 106.6297},
    {"name": "Ho Chi Minh City", "station": "District 3", "lat": 10.7756, "lon": 106.6817},
    {"name": "Ho Chi Minh City", "station": "District 7", "lat": 10.7378, "lon": 106.7220},
    {"name": "Ho Chi Minh City", "station": "Binh Thanh", "lat": 10.8022, "lon": 106.7136},
    {"name": "Ho Chi Minh City", "station": "Tan Binh", "lat": 10.8142, "lon": 106.6438},
    
    {"name": "Da Nang", "station": "Hai Chau", "lat": 16.0544, "lon": 108.2022},
    {"name": "Da Nang", "station": "Son Tra", "lat": 16.1023, "lon": 108.2442},
    {"name": "Da Nang", "station": "Ngu Hanh Son", "lat": 15.9793, "lon": 108.2511},
    
    {"name": "Can Tho", "station": "Ninh Kieu", "lat": 10.0452, "lon": 105.7469},
    {"name": "Can Tho", "station": "Cai Rang", "lat": 10.0211, "lon": 105.7851},
    
    # Other major cities
    {"name": "Hai Phong", "station": "Hong Bang", "lat": 20.8449, "lon": 106.6881},
    {"name": "Bien Hoa", "station": "Dong Nai", "lat": 10.9460, "lon": 106.8234},
    {"name": "Hue", "station": "Hue City", "lat": 16.4674, "lon": 107.5905},
    {"name": "Nha Trang", "station": "Khanh Hoa", "lat": 12.2388, "lon": 109.1967},
    {"name": "Vung Tau", "station": "Ba Ria", "lat": 10.4462, "lon": 107.0840},
    
    # Provincial capitals
    {"name": "An Giang", "station": "Long Xuyen", "lat": 10.3833, "lon": 105.4358},
    {"name": "Bac Giang", "station": "Bac Giang City", "lat": 21.2731, "lon": 106.1946},
    {"name": "Bac Kan", "station": "Bac Kan City", "lat": 22.1477, "lon": 105.8348},
    {"name": "Bac Lieu", "station": "Bac Lieu City", "lat": 9.2851, "lon": 105.7244},
    {"name": "Bac Ninh", "station": "Bac Ninh City", "lat": 21.1861, "lon": 106.0763},
    {"name": "Ben Tre", "station": "Ben Tre City", "lat": 10.2433, "lon": 106.3757},
    {"name": "Binh Dinh", "station": "Quy Nhon", "lat": 13.7722, "lon": 109.2197},
    {"name": "Binh Duong", "station": "Thu Dau Mot", "lat": 10.9804, "lon": 106.6519},
    {"name": "Binh Phuoc", "station": "Dong Xoai", "lat": 11.5369, "lon": 106.9009},
    {"name": "Binh Thuan", "station": "Phan Thiet", "lat": 10.9287, "lon": 108.1023},
    {"name": "Ca Mau", "station": "Ca Mau City", "lat": 9.1767, "lon": 105.1524},
    {"name": "Cao Bang", "station": "Cao Bang City", "lat": 22.6663, "lon": 106.2593},
    {"name": "Dak Lak", "station": "Buon Ma Thuot", "lat": 12.6681, "lon": 108.0378},
    {"name": "Dak Nong", "station": "Gia Nghia", "lat": 12.0342, "lon": 107.6919},
    {"name": "Dien Bien", "station": "Dien Bien Phu", "lat": 21.3880, "lon": 103.0200},
    {"name": "Dong Nai", "station": "Bien Hoa", "lat": 10.9460, "lon": 106.8234},
    {"name": "Dong Thap", "station": "Cao Lanh", "lat": 10.4596, "lon": 105.6325},
    {"name": "Gia Lai", "station": "Pleiku", "lat": 14.0041, "lon": 108.0062},
    {"name": "Ha Giang", "station": "Ha Giang City", "lat": 22.8025, "lon": 104.9784},
    {"name": "Ha Nam", "station": "Phu Ly", "lat": 20.5401, "lon": 105.9109},
    {"name": "Ha Tinh", "station": "Ha Tinh City", "lat": 18.3559, "lon": 105.9058},
    {"name": "Hau Giang", "station": "Vi Thanh", "lat": 9.7838, "lon": 105.4695},
    {"name": "Hoa Binh", "station": "Hoa Binh City", "lat": 20.8065, "lon": 105.3388},
    {"name": "Hung Yen", "station": "Hung Yen City", "lat": 20.6464, "lon": 106.0516},
    {"name": "Khanh Hoa", "station": "Nha Trang", "lat": 12.2388, "lon": 109.1967},
    {"name": "Kien Giang", "station": "Rach Gia", "lat": 10.0120, "lon": 105.0808},
    {"name": "Kon Tum", "station": "Kon Tum City", "lat": 14.3497, "lon": 108.0000},
    {"name": "Lai Chau", "station": "Lai Chau City", "lat": 22.3964, "lon": 103.4707},
    {"name": "Lam Dong", "station": "Da Lat", "lat": 11.9404, "lon": 108.4583},
    {"name": "Lang Son", "station": "Lang Son City", "lat": 21.8523, "lon": 106.7615},
    {"name": "Lao Cai", "station": "Lao Cai City", "lat": 22.4856, "lon": 103.9707},
    {"name": "Long An", "station": "Tan An", "lat": 10.5359, "lon": 106.4137},
    {"name": "Nam Dinh", "station": "Nam Dinh City", "lat": 20.4341, "lon": 106.1675},
    {"name": "Nghe An", "station": "Vinh", "lat": 18.6700, "lon": 105.6900},
    {"name": "Ninh Binh", "station": "Ninh Binh City", "lat": 20.2506, "lon": 105.9744},
    {"name": "Ninh Thuan", "station": "Phan Rang", "lat": 11.5923, "lon": 108.9907},
    {"name": "Phu Tho", "station": "Viet Tri", "lat": 21.4208, "lon": 105.2045},
    {"name": "Phu Yen", "station": "Tuy Hoa", "lat": 13.0881, "lon": 109.0928},
    {"name": "Quang Binh", "station": "Dong Hoi", "lat": 17.6102, "lon": 106.3487},
    {"name": "Quang Nam", "station": "Tam Ky", "lat": 15.5394, "lon": 108.0191},
    {"name": "Quang Nam", "station": "Hoi An", "lat": 15.8801, "lon": 108.3380},
    {"name": "Quang Ngai", "station": "Quang Ngai City", "lat": 15.1214, "lon": 108.8044},
    {"name": "Quang Ninh", "station": "Ha Long", "lat": 20.9101, "lon": 107.1839},
    {"name": "Quang Ninh", "station": "Mong Cai", "lat": 21.5215, "lon": 108.0441},
    {"name": "Quang Tri", "station": "Dong Ha", "lat": 16.7943, "lon": 107.1851},
    {"name": "Soc Trang", "station": "Soc Trang City", "lat": 9.6003, "lon": 105.9800},
    {"name": "Son La", "station": "Son La City", "lat": 21.3256, "lon": 103.9188},
    {"name": "Tay Ninh", "station": "Tay Ninh City", "lat": 11.3100, "lon": 106.0983},
    {"name": "Thai Binh", "station": "Thai Binh City", "lat": 20.4500, "lon": 106.3367},
    {"name": "Thai Nguyen", "station": "Thai Nguyen City", "lat": 21.5944, "lon": 105.8480},
    {"name": "Thai Nguyen", "station": "Song Cong", "lat": 21.4742, "lon": 105.8340},
    {"name": "Thanh Hoa", "station": "Thanh Hoa City", "lat": 19.8077, "lon": 105.7851},
    {"name": "Thanh Hoa", "station": "Sam Son", "lat": 19.7463, "lon": 105.9058},
    {"name": "Thua Thien-Hue", "station": "Hue City", "lat": 16.4674, "lon": 107.5905},
    {"name": "Tien Giang", "station": "My Tho", "lat": 10.3592, "lon": 106.3621},
    {"name": "Tra Vinh", "station": "Tra Vinh City", "lat": 9.9477, "lon": 106.3458},
    {"name": "Tuyen Quang", "station": "Tuyen Quang City", "lat": 21.8273, "lon": 105.2180},
    {"name": "Vinh Long", "station": "Vinh Long City", "lat": 10.2397, "lon": 105.9571},
    {"name": "Vinh Phuc", "station": "Vinh Yen", "lat": 21.3609, "lon": 105.5474},
    {"name": "Yen Bai", "station": "Yen Bai City", "lat": 21.7168, "lon": 104.9113},
]

# Build CITIES list
CITIES = []
for station in CITIES_WITH_COORDS:
    station_id = f"{station['name']} - {station['station']}"
    CITIES.append((station_id, station['name'], station['lat'], station['lon']))

logger.info(f"Total stations configured: {len(CITIES)}")


def wait_for_dependencies():
    """Wait for MinIO and Postgres to be ready"""
    logger.info("Waiting for dependencies to be ready...")
    
    # Wait for Postgres
    max_retries = 30
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST, port=POSTGRES_PORT, connect_timeout=5
            )
            conn.close()
            logger.info("PostgreSQL is ready!")
            break
        except Exception as e:
            logger.info(f"Waiting for PostgreSQL... ({i+1}/{max_retries})")
            time.sleep(5)
    else:
        logger.error("PostgreSQL not ready after 150 seconds")
        return False
    
    # Wait for MinIO
    for i in range(max_retries):
        try:
            client = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
            # Try to list buckets
            client.list_buckets()
            logger.info("MinIO is ready!")
            break
        except Exception as e:
            logger.info(f"Waiting for MinIO... ({i+1}/{max_retries})")
            time.sleep(5)
    else:
        logger.error("MinIO not ready after 150 seconds")
        return False
    
    return True


def save_raw_to_minio(raw_payload, city_label, measured_at=None):
    """Save raw data to MinIO"""
    try:
        client = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)

        timestamp = measured_at.isoformat() if hasattr(measured_at, 'isoformat') and measured_at is not None else datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        safe_city = city_label.replace(' ', '_').replace('-', '_')
        filename = f"waqi_raw/city={safe_city}/year={timestamp[:4]}/month={timestamp[5:7]}/day={timestamp[8:10]}/{timestamp}.json"
        data = json.dumps(raw_payload).encode('utf-8')
        client.put_object(BUCKET, filename, io.BytesIO(data), length=len(data), content_type='application/json')
        logger.info(f"Saved raw payload to MinIO: {filename}")
    except Exception as e:
        logger.error(f"MinIO save error for {city_label}: {e}")


def fetch_waqi_feed(city_name):
    """Fetch data from WAQI API"""
    url = f"{BASE_URL}/feed/{city_name}/"
    try:
        r = requests.get(url, params={'token': WAQI_TOKEN}, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"WAQI error for {city_name}: {e}")
        return None


def upsert_station_and_insert_measurement(payload, conn, station_id, city_name, lat, lon):
    """Insert station and measurement data to PostgreSQL"""
    cur = conn.cursor()
    data = payload.get('data', {})
    if not data:
        cur.close()
        return

    try:
        # Find or create station
        cur.execute("SELECT station_id FROM dim_air_quality_stations WHERE station_name = %s LIMIT 1", (station_id,))
        row = cur.fetchone()
        
        if not row:
            # Try to insert new station, handle duplicate waqi_id
            try:
                cur.execute("""
                    INSERT INTO dim_air_quality_stations 
                    (waqi_id, station_name, city, country, latitude, longitude, source_url, attribution, latest_reading_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING station_id
                """, (
                    data.get('idx'), station_id, city_name, 'Vietnam', lat, lon,
                    'https://waqi.info/', json.dumps(data.get('attributions', [])), datetime.utcnow()
                ))
                db_station_id = cur.fetchone()[0]
            except Exception as e:
                # If waqi_id already exists, find the existing station
                conn.rollback()
                waqi_id = data.get('idx')
                cur.execute("SELECT station_id FROM dim_air_quality_stations WHERE waqi_id = %s LIMIT 1", (waqi_id,))
                existing_row = cur.fetchone()
                if existing_row:
                    db_station_id = existing_row[0]
                    logger.debug(f"Using existing station with waqi_id {waqi_id}")
                else:
                    # Insert without waqi_id to avoid conflict
                    cur.execute("""
                        INSERT INTO dim_air_quality_stations 
                        (station_name, city, country, latitude, longitude, source_url, attribution, latest_reading_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING station_id
                    """, (
                        station_id, city_name, 'Vietnam', lat, lon,
                        'https://waqi.info/', json.dumps(data.get('attributions', [])), datetime.utcnow()
                    ))
                    db_station_id = cur.fetchone()[0]
        else:
            db_station_id = row[0]

        # Insert measurement
        iaqi = data.get('iaqi', {})
        def get_val(key):
            val = iaqi.get(key, {}).get('v') if isinstance(iaqi.get(key), dict) else None
            # Handle string values like "-" that should be null
            if val == "-" or val == "" or val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Handle AQI value
        aqi_val = data.get('aqi')
        if aqi_val == "-" or aqi_val == "" or aqi_val is None:
            aqi_val = None
        else:
            try:
                aqi_val = int(aqi_val)
            except (ValueError, TypeError):
                aqi_val = None

        # Check if measurement already exists
        cur.execute("""
            SELECT 1 FROM fact_air_quality_measurements 
            WHERE station_id = %s AND measured_at = %s
        """, (db_station_id, data.get('time', {}).get('iso')))
        
        if cur.fetchone():
            logger.debug(f"Measurement already exists for {station_id} at {data.get('time', {}).get('iso')}")
            conn.commit()
            cur.close()
            return
        
        # Insert new measurement
        cur.execute("""
            INSERT INTO fact_air_quality_measurements 
            (station_id, measured_at, aqi, co, no2, o3, pm10, pm25, so2, temperature, humidity, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            db_station_id,
            data.get('time', {}).get('iso'),
            aqi_val,
            get_val('co'), get_val('no2'), get_val('o3'),
            get_val('pm10'), get_val('pm25'), get_val('so2'),
            get_val('t'), get_val('h'),
            json.dumps(payload)
        ))
        
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"Database error for {station_id}: {e}")
        conn.rollback()
        cur.close()
        raise


def run_once():
    """Main streaming function - fetch data for all stations"""
    logger.info("Starting data collection round...")
    fetched = 0
    errors = 0
    
    for station_id, city_name, lat, lon in CITIES:
        logger.debug(f"Processing: {station_id}")
        
        # Try WAQI API
        feed = fetch_waqi_feed(city_name)
        if not feed or feed.get('status') != 'ok':
            logger.warning(f"No WAQI data for {station_id}")
            errors += 1
            continue
        
        # Save raw data to MinIO
        save_raw_to_minio(feed, station_id)
        
        # Save to Postgres
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST, port=POSTGRES_PORT
            )
            upsert_station_and_insert_measurement(feed, conn, station_id, city_name, lat, lon)
            conn.close()
            fetched += 1
            logger.debug(f"[OK] {station_id}")
        except Exception as e:
            logger.error(f"[ERROR] DB insert for {station_id}: {e}")
            errors += 1
        
        time.sleep(1)  # Rate limiting between requests
    
    logger.info(f"Streaming round completed: {fetched} stations processed, {errors} errors")
    return fetched, errors


def health_check():
    """Simple health check - test database and MinIO connectivity"""
    try:
        # Test PostgreSQL
        conn = psycopg2.connect(
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST, port=POSTGRES_PORT, connect_timeout=5
        )
        conn.close()
        
        # Test MinIO
        client = Minio(MINIO_HOST, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
        client.list_buckets()
        
        return True
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


# Main execution
if __name__ == '__main__':
    logger.info("=== Air Quality Streaming Service ===")
    logger.info(f"Configured stations: {len(CITIES)}")
    logger.info(f"WAQI Token: {'Set' if WAQI_TOKEN else 'Missing'}")
    logger.info(f"Streaming interval: {STREAMING_INTERVAL} seconds")
    
    # Wait for dependencies
    if not wait_for_dependencies():
        logger.error("Dependencies not ready, exiting")
        exit(1)
    
    # Initial health check
    if not health_check():
        logger.error("Initial health check failed, exiting")
        exit(1)
    
    logger.info("Starting streaming loop...")
    
    # Main streaming loop
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    try:
        while True:
            start_time = time.time()
            
            try:
                fetched, errors = run_once()
                
                # Reset consecutive errors on successful run
                if fetched > 0:
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
                
                # Check if too many consecutive errors
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({consecutive_errors}), exiting")
                    break
                
            except Exception as e:
                logger.error(f"Streaming round failed: {e}")
                consecutive_errors += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({consecutive_errors}), exiting")
                    break
            
            # Calculate sleep time
            elapsed = time.time() - start_time
            sleep_time = max(0, STREAMING_INTERVAL - elapsed)
            
            if sleep_time > 0:
                logger.info(f"Sleeping {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, stopping...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        exit(1)
    
    logger.info("Streaming service stopped")