import os
import sys
import io
import time
import json
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from minio import Minio

# =============================
# 0) Load ENV & constants
# =============================
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

MINIO_HOST   = os.environ.get("MINIO_HOST", "localhost:9004")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "air-quality")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"


# =============================
# 1) Helpers
# =============================
def get_minio_client() -> Minio:
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def load_locations(path: str) -> List[Dict]:
    if not path:
        raise ValueError("❌ You must provide --locations path (JSONL or JSON).")

    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()

    lines = content.splitlines()
    # JSONL
    if len(lines) > 1:
        locs = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                locs.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        if locs:
            return locs

    # JSON thường
    data = json.loads(content)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "locations" in data:
        return data["locations"]

    raise ValueError(f"Invalid locations format in {path}")


def generate_date_chunks(start: str, end: str, days: int = 30):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    current = s
    while current <= e:
        chunk_end = min(current + timedelta(days=days - 1), e)
        yield current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        current = chunk_end + timedelta(days=1)


def backoff_sleep(attempt: int):
    time.sleep(min(2 ** attempt, 10))


# =============================
# 2) Fetch API
# =============================
def fetch_openmeteo(lat: float, lon: float, start_date: str, end_date: str, max_retries: int = 3) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": [
            "pm2_5", "pm10", "nitrogen_dioxide", "ozone",
            "sulphur_dioxide", "carbon_monoxide",
            "aerosol_optical_depth", "dust", "uv_index", "carbon_dioxide",
            "us_aqi", "us_aqi_pm2_5", "us_aqi_pm10",
            "us_aqi_nitrogen_dioxide", "us_aqi_ozone",
            "us_aqi_sulphur_dioxide", "us_aqi_carbon_monoxide"
        ],
        "timezone": "UTC",
        "start_date": start_date,
        "end_date": end_date,
    }

    for attempt in range(max_retries):
        try:
            r = requests.get(API_URL, params=params, timeout=30)
            r.raise_for_status()
            js = r.json()
            break
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"API error ({lat},{lon},{start_date}→{end_date}): {e}")
                return pd.DataFrame()
            backoff_sleep(attempt)

    if "hourly" not in js or not js["hourly"]:
        return pd.DataFrame()

    df = pd.DataFrame(js["hourly"])
    if "time" not in df:
        return pd.DataFrame()

    df["ts_utc"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["date_utc"] = df["ts_utc"].dt.date
    df.drop(columns=["time"], inplace=True, errors="ignore")

    df["latitude"] = js.get("latitude")
    df["longitude"] = js.get("longitude")
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC")

    now_utc = pd.Timestamp.now(tz="UTC")
    df = df[df["ts_utc"].notna()]
    df = df[df["ts_utc"] <= now_utc]

    rename_map = {
        "pm2_5": "pm25", "nitrogen_dioxide": "no2", "ozone": "o3",
        "sulphur_dioxide": "so2", "carbon_monoxide": "co",
        "aerosol_optical_depth": "aod", "carbon_dioxide": "co2",
        "us_aqi": "aqi", "us_aqi_pm2_5": "aqi_pm25", "us_aqi_pm10": "aqi_pm10",
        "us_aqi_nitrogen_dioxide": "aqi_no2", "us_aqi_ozone": "aqi_o3",
        "us_aqi_sulphur_dioxide": "aqi_so2", "us_aqi_carbon_monoxide": "aqi_co"
    }
    df.rename(columns=rename_map, inplace=True)

    front_cols = ["ts_utc", "date_utc", "latitude", "longitude"]
    df = df[front_cols + [c for c in df.columns if c not in front_cols]]

    return df.reset_index(drop=True)


# =============================
# 3) Save to MinIO (by year)
# =============================
def save_year_to_minio(year: int, df: pd.DataFrame):
    """
    Ghi 1 file CSV cho đúng 1 năm.
    Đường dẫn: openmeteo/{year}/openmeteo_{year}.csv
    """
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    object_name = f"openmeteo/{year}/openmeteo_{year}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    csv_stream = io.BytesIO(csv_bytes)

    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=csv_stream,
        length=len(csv_bytes),
        content_type="text/csv",
    )
    print(f"✅ Uploaded year {year}: s3://{MINIO_BUCKET}/{object_name}")


# =============================
# 4) Ingestion
# =============================
def run_backfill(locations: List[Dict], start_date: str, end_date: str, chunk_days: int = 30):
    """
    Khác bản cũ: ta sẽ gom toàn bộ dữ liệu về 1 dict theo năm,
    rồi cuối cùng mới ghi 1 file / năm.
    """
    # year -> list of dataframes
    yearly_data: Dict[int, List[pd.DataFrame]] = {}

    for loc in locations:
        lat = loc["latitude"]
        lon = loc["longitude"]
        loc_key = loc.get("location_key") or f"{lat}_{lon}"
        print(f"\n==> Location: {loc_key}")

        for s, e in generate_date_chunks(start_date, end_date, days=chunk_days):
            print(f"  Fetching {s} → {e}")
            df = fetch_openmeteo(lat, lon, s, e)
            if df.empty:
                print(f"    ⚠️ No data for {loc_key} {s}→{e}")
                continue

            # gắn lại khóa để downstream biết là của trạm nào
            df["location_key"] = loc_key

            # thêm cột year để group
            df["year_utc"] = pd.to_datetime(df["ts_utc"]).dt.year

            # dồn vào dict theo năm
            for year, g in df.groupby("year_utc"):
                yearly_data.setdefault(year, []).append(g)

            time.sleep(10)

    # sau khi duyệt hết location -> ghi ra từng năm
    for year, df_list in yearly_data.items():
        big_df = pd.concat(df_list, ignore_index=True)
        save_year_to_minio(year, big_df)

    print("\nBACKFILL COMPLETE.")
    print(f"Years exported: {list(yearly_data.keys())}")


def run_incremental(locations: List[Dict]):
    """
    Incremental: vẫn sẽ ghi vào folder của năm hiện tại.
    Nghĩa là mỗi lần chạy sẽ tạo lại file năm hiện tại (overwrite).
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    year_now = datetime.utcnow().year

    yearly_data: Dict[int, List[pd.DataFrame]] = {}

    for loc in locations:
        lat = loc["latitude"]
        lon = loc["longitude"]
        loc_key = loc.get("location_key") or f"{lat}_{lon}"
        print(f"\n==> Location: {loc_key} (today: {today})")

        df = fetch_openmeteo(lat, lon, today, today)
        if df.empty:
            print(f"  ⚠️ No data for {loc_key} today")
            continue

        df["location_key"] = loc_key
        df["year_utc"] = pd.to_datetime(df["ts_utc"]).dt.year

        yearly_data.setdefault(year_now, []).append(df)

        time.sleep(10)

    # ghi lại năm hiện tại
    if year_now in yearly_data:
        big_df = pd.concat(yearly_data[year_now], ignore_index=True)
        save_year_to_minio(year_now, big_df)

    print("\nINCREMENTAL COMPLETE.")


# =============================
# 5) CLI
# =============================
def main():
    parser = argparse.ArgumentParser(description="Bronze ingestion for Open-Meteo → MinIO (year-partitioned CSV)")
    parser.add_argument("--mode", choices=["backfill", "incremental"], required=True)
    parser.add_argument("--start-date", help="YYYY-MM-DD (required for backfill)")
    parser.add_argument("--end-date", help="YYYY-MM-DD (default=today)")
    parser.add_argument("--chunk-days", type=int, default=30)
    parser.add_argument("--locations", required=True, help="Path to JSON/JSONL file of locations")
    args = parser.parse_args()

    locations = load_locations(args.locations)

    if args.mode == "backfill":
        if not args.start_date:
            parser.error("--start-date is required for backfill")
        end_date = args.end_date or datetime.utcnow().strftime("%Y-%m-%d")
        run_backfill(locations, args.start_date, end_date, args.chunk_days)
    else:
        run_incremental(locations)

if __name__ == "__main__":
    main()
