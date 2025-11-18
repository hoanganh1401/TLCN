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


def generate_date_chunks(start: str, end: str, days: int = 60):
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

    # chuẩn hóa thời gian
    df["ts_utc"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["date_utc"] = df["ts_utc"].dt.date
    df.drop(columns=["time"], inplace=True, errors="ignore")

    # thêm tọa độ + thời điểm nạp
    df["latitude"] = js.get("latitude")
    df["longitude"] = js.get("longitude")
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC")

    # loại bỏ dòng tương lai
    now_utc = pd.Timestamp.now(tz="UTC")
    df = df[df["ts_utc"].notna()]
    df = df[df["ts_utc"] <= now_utc]

    # đổi tên cột
    rename_map = {
        "pm2_5": "pm25", "nitrogen_dioxide": "no2", "ozone": "o3",
        "sulphur_dioxide": "so2", "carbon_monoxide": "co",
        "aerosol_optical_depth": "aod", "carbon_dioxide": "co2",
        "us_aqi": "aqi", "us_aqi_pm2_5": "aqi_pm25", "us_aqi_pm10": "aqi_pm10",
        "us_aqi_nitrogen_dioxide": "aqi_no2", "us_aqi_ozone": "aqi_o3",
        "us_aqi_sulphur_dioxide": "aqi_so2", "us_aqi_carbon_monoxide": "aqi_co"
    }
    df.rename(columns=rename_map, inplace=True)

    # sắp cột
    front_cols = ["ts_utc", "date_utc", "latitude", "longitude"]
    df = df[front_cols + [c for c in df.columns if c not in front_cols]]

    return df.reset_index(drop=True)


# =============================
# 3) Save to MinIO (merge by day)
# =============================
def save_daily_to_minio_merge(client: Minio, df: pd.DataFrame):
    """
    Ghi 1 file CSV cho từng ngày.
    Nếu file ngày đó đã tồn tại thì đọc về, nối thêm (concat) rồi ghi đè lại.
    Đường dẫn: openmeteo/{year}/{month}/{day}/openmeteo_{YYYY_MM_DD}.csv
    """
    ensure_bucket(client, MINIO_BUCKET)

    for date_utc, g in df.groupby("date_utc"):
        year = date_utc.year
        month = f"{date_utc.month:02d}"
        day = f"{date_utc.day:02d}"
        object_name = f"openmeteo/{year}/{month}/{day}/openmeteo_{year}_{month}_{day}.csv"

        # dữ liệu mới
        new_df = g

        # mặc định: dữ liệu để ghi là dữ liệu mới
        merged_df = new_df

        # thử đọc dữ liệu cũ nếu có
        try:
            resp = client.get_object(MINIO_BUCKET, object_name)
            old_bytes = resp.read()
            old_df = pd.read_csv(io.BytesIO(old_bytes))

            # nối
            merged_df = pd.concat([old_df, new_df], ignore_index=True)

            # tránh trùng theo (ts_utc, location_key) nếu có
            if "location_key" in merged_df.columns:
                merged_df = merged_df.drop_duplicates(
                    subset=["ts_utc", "location_key"],
                    keep="last"
                )
            else:
                merged_df = merged_df.drop_duplicates()
        except Exception:
            # không có file cũ thì thôi
            pass

        # ghi lại
        csv_bytes = merged_df.to_csv(index=False).encode("utf-8")
        csv_stream = io.BytesIO(csv_bytes)

        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=csv_stream,
            length=len(csv_bytes),
            content_type="text/csv",
        )

        print(f"✅ Uploaded/Merged {year}-{month}-{day}: s3://{MINIO_BUCKET}/{object_name} (rows={len(merged_df)})")


# =============================
# 4) Ingestion
# =============================
def run_backfill(locations: List[Dict], start_date: str, end_date: str, chunk_days: int = 60):
    """
    Backfill toàn bộ dữ liệu, lưu theo ngày.
    Mỗi ngày chỉ có 1 file, trong đó chứa tất cả location.
    """
    client = get_minio_client()

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

            df["location_key"] = loc_key
            save_daily_to_minio_merge(client, df)

            # tránh spam API
            time.sleep(0.5)

    print("\nBACKFILL COMPLETE.")


def run_incremental(locations: List[Dict]):
    """
    Incremental: chỉ lấy dữ liệu của ngày hiện tại.
    Vẫn merge vào file ngày đó để gom tất cả tỉnh lại.
    """
    client = get_minio_client()
    today = datetime.utcnow().strftime("%Y-%m-%d")

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
        save_daily_to_minio_merge(client, df)

        time.sleep(0.5)

    print("\nINCREMENTAL COMPLETE.")


# =============================
# 5) CLI
# =============================
def main():
    parser = argparse.ArgumentParser(description="Bronze ingestion for Open-Meteo → MinIO (day-partitioned CSV, merged)")
    parser.add_argument("--mode", choices=["backfill", "incremental"], required=True)
    parser.add_argument("--start-date", help="YYYY-MM-DD (required for backfill)")
    parser.add_argument("--end-date", help="YYYY-MM-DD (default=today)")
    parser.add_argument("--chunk-days", type=int, default=60)
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
