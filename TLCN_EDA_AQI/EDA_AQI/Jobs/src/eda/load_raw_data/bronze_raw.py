import os
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
# 0) Custom exception + Load ENV & constants
# =============================
class RateLimitError(Exception):
    """Raised when Open-Meteo returns 429 Too Many Requests after retries."""
    pass


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

MINIO_HOST = os.environ.get("MINIO_HOST", "localhost:9004")
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


def generate_date_chunks(start: str, end: str, days: int = 120):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    current = s
    while current <= e:
        chunk_end = min(current + timedelta(days=days - 1), e)
        yield current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        current = chunk_end + timedelta(days=1)


def backoff_sleep(attempt: int):
    """Exponential backoff: 2^attempt (max 10 seconds)."""
    time.sleep(min(2 ** attempt, 10))


def _normalize_ts_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure ts_utc is timezone-aware UTC datetime."""
    if df is None or df.empty:
        return df
    if "ts_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    return df


# =============================
# 2) Fetch API (handle 429)
# =============================
def fetch_openmeteo(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    max_retries: int = 3,
) -> pd.DataFrame:
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

    last_error: Optional[Exception] = None
    js: Optional[dict] = None

    for attempt in range(max_retries):
        try:
            r = requests.get(API_URL, params=params, timeout=30)
            r.raise_for_status()
            js = r.json()
            break

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None

            if status == 429:
                print(
                    f"[WARN] 429 Too Many Requests for "
                    f"({lat},{lon},{start_date}→{end_date}), "
                    f"attempt {attempt + 1}/{max_retries}"
                )
                last_error = e
                if attempt == max_retries - 1:
                    raise RateLimitError(
                        f"Rate limit (429) for ({lat},{lon},{start_date}→{end_date})"
                    ) from e
                backoff_sleep(attempt + 1)
                continue

            print(
                f"[ERROR] HTTP {status} for "
                f"({lat},{lon},{start_date}→{end_date}): {e}"
            )
            last_error = e
            if attempt == max_retries - 1:
                return pd.DataFrame()
            backoff_sleep(attempt + 1)

        except Exception as e:
            print(
                f"[ERROR] Non-HTTP error for "
                f"({lat},{lon},{start_date}→{end_date}): {e}"
            )
            last_error = e
            if attempt == max_retries - 1:
                return pd.DataFrame()
            backoff_sleep(attempt + 1)

    if not isinstance(js, dict):
        print(
            f"[ERROR] Unexpected response for "
            f"({lat},{lon},{start_date}→{end_date}): {last_error}"
        )
        return pd.DataFrame()

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
# 3) Save to MinIO (merge by day)
#    - Backfill: merge full
#    - Incremental: append only new rows per location
# =============================
def save_daily_to_minio_merge(
    client: Minio,
    df: pd.DataFrame,
    incremental_append_only: bool = False,
):
    """
    Write 1 CSV per day:
      openmeteo/{year}/{month}/{day}/openmeteo_{YYYY_MM_DD}.csv

    - Always: normalize ts_utc, dedup by (ts_utc, location_key), stable sort.
    - If incremental_append_only=True:
        only append rows that have ts_utc > max(ts_utc) already stored per location.
    """
    ensure_bucket(client, MINIO_BUCKET)

    for date_utc, g in df.groupby("date_utc"):
        year = date_utc.year
        month = f"{date_utc.month:02d}"
        day = f"{date_utc.day:02d}"
        object_name = f"openmeteo/{year}/{month}/{day}/openmeteo_{year}_{month}_{day}.csv"

        new_df = g.copy()
        new_df = _normalize_ts_utc(new_df)

        merged_df = new_df

        try:
            resp = client.get_object(MINIO_BUCKET, object_name)
            try:
                old_bytes = resp.read()
            finally:
                try:
                    resp.close()
                    resp.release_conn()
                except Exception:
                    pass

            old_df = pd.read_csv(io.BytesIO(old_bytes), parse_dates=["ts_utc"])
            old_df = _normalize_ts_utc(old_df)

            if incremental_append_only and not old_df.empty:
                last_ts = old_df.groupby("location_key")["ts_utc"].max().to_dict()
                # keep only rows newer than last_ts for that location
                new_df = new_df[
                    new_df["ts_utc"] > new_df["location_key"].map(last_ts).fillna(pd.Timestamp.min.tz_localize("UTC"))
                ]

            merged_df = pd.concat([old_df, new_df], ignore_index=True)

            if "location_key" in merged_df.columns and "ts_utc" in merged_df.columns:
                merged_df = merged_df.drop_duplicates(
                    subset=["ts_utc", "location_key"],
                    keep="last",
                )
            else:
                merged_df = merged_df.drop_duplicates(keep="last")

        except Exception:
            pass

        sort_cols = []
        if "ts_utc" in merged_df.columns:
            sort_cols.append("ts_utc")
        if "location_order" in merged_df.columns:
            sort_cols.append("location_order")

        if sort_cols:
            merged_df = merged_df.sort_values(
                by=sort_cols,
                ascending=[True] * len(sort_cols),
                kind="mergesort",
            ).reset_index(drop=True)

        # keep file clean
        merged_df = merged_df.drop(columns=["location_order"], errors="ignore")

        csv_bytes = merged_df.to_csv(index=False).encode("utf-8")

        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=io.BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="text/csv",
        )

        print(
            f"✅ Uploaded/Merged {year}-{month}-{day}: "
            f"s3://{MINIO_BUCKET}/{object_name} (rows={len(merged_df)})"
        )


# =============================
# 4) Ingestion
# =============================
def run_backfill(
    locations: List[Dict],
    start_date: str,
    end_date: str,
    chunk_days: int = 120,
):
    client = get_minio_client()

    # location order follows input file order
    loc_order = {
        (loc.get("location_key") or f"{loc['latitude']}_{loc['longitude']}"): i
        for i, loc in enumerate(locations)
    }

    for loc in locations:
        lat = loc["latitude"]
        lon = loc["longitude"]
        loc_key = loc.get("location_key") or f"{lat}_{lon}"
        print(f"\n==> Location: {loc_key}")

        for s, e in generate_date_chunks(start_date, end_date, days=chunk_days):
            print(f"  Fetching {s} → {e}")
            try:
                df = fetch_openmeteo(lat, lon, s, e)
            except RateLimitError as ex:
                print(f"⛔ Hit rate limit while backfilling {loc_key} {s}→{e}: {ex}")
                print("   Stopping backfill early. Re-run later to continue.")
                return

            if df.empty:
                print(f"    ⚠️ No data for {loc_key} {s}→{e}")
                continue

            df["location_key"] = loc_key
            df["location_order"] = loc_order.get(loc_key, 10**9)

            # backfill merges full (not append-only)
            save_daily_to_minio_merge(client, df, incremental_append_only=False)
            time.sleep(0.5)

    print("\nBACKFILL COMPLETE.")


def run_incremental(locations: List[Dict]):
    client = get_minio_client()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    loc_order = {
        (loc.get("location_key") or f"{loc['latitude']}_{loc['longitude']}"): i
        for i, loc in enumerate(locations)
    }

    for loc in locations:
        lat = loc["latitude"]
        lon = loc["longitude"]
        loc_key = loc.get("location_key") or f"{lat}_{lon}"
        print(f"\n==> Location: {loc_key} (today: {today})")

        try:
            df = fetch_openmeteo(lat, lon, today, today)
        except RateLimitError as ex:
            print(f"⛔ Hit Open-Meteo rate limit while fetching {loc_key}: {ex}")
            print("   Stopping incremental run to avoid spamming the API.")
            break

        if df.empty:
            print(f"  ⚠️ No data for {loc_key} today")
            continue

        df["location_key"] = loc_key
        df["location_order"] = loc_order.get(loc_key, 10**9)

        # incremental: append only new rows per location
        save_daily_to_minio_merge(client, df, incremental_append_only=True)
        time.sleep(0.5)

    print("\nINCREMENTAL COMPLETE.")


# =============================
# 5) CLI
# =============================
def main():
    parser = argparse.ArgumentParser(
        description="Bronze ingestion for Open-Meteo → MinIO (day CSV, merge/idempotent)"
    )
    parser.add_argument("--mode", choices=["backfill", "incremental"], required=True)
    parser.add_argument("--start-date", help="YYYY-MM-DD (required for backfill)")
    parser.add_argument("--end-date", help="YYYY-MM-DD (default=today)")
    parser.add_argument("--chunk-days", type=int, default=120)
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
