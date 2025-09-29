"""
Transform WAQI raw JSON in MinIO -> per-city/date CSV files under dags/Processed-data

Behaviour
- Reads JSON objects from a MinIO bucket/prefix (defaults to bucket=air-quality, prefix=waqi_raw)
- Normalizes selected IAQI fields and metadata into a flat CSV row
- Writes CSVs to `dags/Processed-data/<city_safe>/<YYYY-MM-DD>.csv`
- Provides a CLI (--dry-run, --minio-*) and an Airflow PythonOperator wrapper

This file intentionally keeps external dependencies minimal (uses stdlib + minio + requests).
"""

from datetime import datetime
import os
import io
import json
import csv
import logging
import argparse

try:
	from minio import Minio
except Exception:
	Minio = None

try:
	from airflow import DAG
	from airflow.operators.python import PythonOperator
	from airflow.utils.dates import days_ago
	AIRFLOW_AVAILABLE = True
except Exception:
	AIRFLOW_AVAILABLE = False


# Configuration via environment (overridable via CLI)
MINIO_HOST = os.environ.get('MINIO_HOST', 'minio:9000')
MINIO_ACCESS = os.environ.get('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET = os.environ.get('MINIO_SECRET_KEY', 'admin123')
BUCKET = os.environ.get('MINIO_BUCKET', 'air-quality')
PREFIX = os.environ.get('MINIO_RAW_PREFIX', 'waqi_raw')

# Local output dir for processed CSVs (repo path)
BASE_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'Processed-data')

CSV_COLUMNS = [
	'city_label',
	'city_name',
	'measured_at',
	'aqi',
	'dominentpol',
	'idx',
	'lat',
	'lon',
	'co',
	'pm25',
	'pm10',
	't',
	'h',
	'o3',
	'p',
	'w',
	'dew'
]


def _get_minio_client(host=MINIO_HOST, access=MINIO_ACCESS, secret=MINIO_SECRET):
	if Minio is None:
		raise RuntimeError('minio package is not available')
	return Minio(host, access_key=access, secret_key=secret, secure=False)


def _safe_city_label(label: str) -> str:
	# Use simple normalization for filesystem paths
	return label.replace('/', '_').replace(' ', '_').replace('-', '_')


def _extract_row_from_payload(payload: dict) -> dict:
	data = payload.get('data', {}) if isinstance(payload, dict) else {}
	city = data.get('city', {})
	city_name = city.get('name') or ''
	city_label = city_name.split(',')[0] if city_name else ''

	time_obj = data.get('time', {})
	measured_iso = time_obj.get('iso') if isinstance(time_obj, dict) else None
	measured_at = measured_iso or None

	iaqi = data.get('iaqi', {}) or {}

	# flatten iaqi values (some keys may be missing)
	def v(k):
		vv = iaqi.get(k)
		if isinstance(vv, dict):
			return vv.get('v')
		return None

	geo = city.get('geo') if isinstance(city.get('geo'), (list, tuple)) else [None, None]
	lat = geo[0] if geo and len(geo) >= 1 else None
	lon = geo[1] if geo and len(geo) >= 2 else None

	row = {
		'city_label': city_label,
		'city_name': city_name,
		'measured_at': measured_at,
		'aqi': data.get('aqi'),
		'dominentpol': data.get('dominentpol'),
		'idx': data.get('idx'),
		'lat': lat,
		'lon': lon,
		'co': v('co'),
		'pm25': v('pm25'),
		'pm10': v('pm10'),
		't': v('t'),
		'h': v('h'),
		'o3': v('o3'),
		'p': v('p'),
		'w': v('w'),
		'dew': v('dew')
	}
	return row


def _ensure_dir(path: str):
	os.makedirs(path, exist_ok=True)


def write_row_to_csv(row: dict, output_base: str = BASE_OUTPUT_DIR, logger: logging.Logger = None):
	city_label = row.get('city_label') or 'unknown'
	safe = _safe_city_label(city_label)
	measured_at = row.get('measured_at')
	date_part = None
	if measured_at:
		try:
			# Accept either ISO with offset or naive
			dt = datetime.fromisoformat(measured_at.replace('Z', '+00:00'))
			date_part = dt.strftime('%Y-%m-%d')
		except Exception:
			date_part = datetime.utcnow().strftime('%Y-%m-%d')
	else:
		date_part = datetime.utcnow().strftime('%Y-%m-%d')

	out_dir = os.path.join(output_base, safe)
	_ensure_dir(out_dir)
	out_file = os.path.join(out_dir, f"{date_part}.csv")

	# Avoid duplicate timestamps in file by checking existing measured_at values
	existing_timestamps = set()
	if os.path.exists(out_file):
		try:
			with open(out_file, 'r', newline='', encoding='utf-8') as f:
				reader = csv.DictReader(f)
				for r in reader:
					existing_timestamps.add(r.get('measured_at'))
		except Exception:
			pass

	if row.get('measured_at') in existing_timestamps:
		if logger:
			logger.debug(f"Skipping duplicate row for {row.get('measured_at')} -> {out_file}")
		return out_file, False

	write_header = not os.path.exists(out_file)
	with open(out_file, 'a', newline='', encoding='utf-8') as f:
		writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
		if write_header:
			writer.writeheader()
		# Ensure we only write columns in CSV_COLUMNS order
		out_row = {k: row.get(k) for k in CSV_COLUMNS}
		writer.writerow(out_row)
	if logger:
		logger.info(f"Wrote row -> {out_file}")
	return out_file, True


def _upload_file_to_minio(local_path: str, minio_client, bucket: str, object_name: str, logger: logging.Logger = None):
	"""Upload a local file to MinIO (overwrites if exists)."""
	try:
		with open(local_path, 'rb') as f:
			data = f.read()
		minio_client.put_object(bucket, object_name, io.BytesIO(data), length=len(data), content_type='text/csv')
		if logger:
			logger.info(f"Uploaded processed CSV to MinIO: s3://{bucket}/{object_name}")
		return True
	except Exception as e:
		if logger:
			logger.error(f"Failed to upload {local_path} to MinIO {object_name}: {e}")
		return False



def _append_row_to_minio_csv(row: dict, minio_client, bucket: str, prefix: str = 'processed-data', logger: logging.Logger = None):
	"""Append a single CSV row to object at processed-data/<city>/<YYYY-MM-DD>.csv in MinIO.
	Because object storage doesn't support append, we download existing CSV (if any), check dupes
	by measured_at, then upload the new CSV content overwriting the object.
	Returns (uploaded: bool, reason:str)
	"""
	city_label = row.get('city_label') or 'unknown'
	safe = _safe_city_label(city_label)
	measured_at = row.get('measured_at')
	# determine date
	if measured_at:
		try:
			dt = datetime.fromisoformat(measured_at.replace('Z', '+00:00'))
			date_part = dt.strftime('%Y-%m-%d')
		except Exception:
			date_part = datetime.utcnow().strftime('%Y-%m-%d')
	else:
		date_part = datetime.utcnow().strftime('%Y-%m-%d')

	object_name = f"{prefix}/{safe}/{date_part}.csv"

	# Ensure bucket exists
	try:
		if not minio_client.bucket_exists(bucket):
			minio_client.make_bucket(bucket)
	except Exception:
		# ignore, will surface on put
		pass

	existing_rows = []
	existing_timestamps = set()
	try:
		# try to download existing object
		obj = minio_client.get_object(bucket, object_name)
		data = obj.read()
		text = data.decode('utf-8')
		# parse CSV
		f = io.StringIO(text)
		reader = csv.DictReader(f)
		for r in reader:
			existing_rows.append(r)
			existing_timestamps.add(r.get('measured_at'))
	except Exception:
		# object does not exist or cannot be read; we'll create new
		existing_rows = []
		existing_timestamps = set()

	if row.get('measured_at') in existing_timestamps:
		if logger:
			logger.debug(f"Skipping duplicate measured_at {row.get('measured_at')} for {object_name}")
		return False, 'duplicate'

	# Append new row dict (ensure keys order)
	out_rows = existing_rows + [{k: (row.get(k) if row.get(k) is not None else '') for k in CSV_COLUMNS}]

	# Write CSV into bytes
	out_io = io.StringIO()
	writer = csv.DictWriter(out_io, fieldnames=CSV_COLUMNS)
	writer.writeheader()
	for r in out_rows:
		writer.writerow(r)
	out_bytes = out_io.getvalue().encode('utf-8')

	try:
		minio_client.put_object(bucket, object_name, io.BytesIO(out_bytes), length=len(out_bytes), content_type='text/csv')
		if logger:
			logger.info(f"Uploaded processed CSV to MinIO: s3://{bucket}/{object_name}")
		return True, 'uploaded'
	except Exception as e:
		if logger:
			logger.error(f"Failed to upload processed CSV to MinIO {object_name}: {e}")
		return False, str(e)


def transform_minio_to_csv(minio_host=MINIO_HOST, access=MINIO_ACCESS, secret=MINIO_SECRET,
						   bucket=BUCKET, prefix=PREFIX, output_base=BASE_OUTPUT_DIR,
						   dry_run=False, upload_to_minio=True, save_local_copy=False, logger: logging.Logger = None):
	if logger is None:
		logger = logging.getLogger('transform')

	if Minio is None:
		logger.error('minio package not installed; cannot run transform')
		return 0, 0

	client = _get_minio_client(minio_host, access, secret)

	# Try several common prefixes so we detect objects regardless of extractor naming.
	# The extractor may write to 'waqi_raw', 'raw/waqi' or under the bucket root like 'air-quality/waqi_raw'.
	common_variants = [
		prefix,
		'raw/waqi',
		'waqi_raw',
		'waqi-raw',
		'waqi/raw',
		'waqi_raw/',
		'air-quality/waqi_raw'
	]
	# de-duplicate while preserving order
	seen = set()
	prefixes_to_try = []
	for x in common_variants:
		if x not in seen:
			seen.add(x)
			prefixes_to_try.append(x)

	total = 0
	written = 0

	for p in prefixes_to_try:
		try:
			objects = client.list_objects(bucket, prefix=p, recursive=True)
		except Exception as e:
			logger.debug(f"Could not list objects for prefix {p}: {e}")
			continue

		for obj in objects:
			# Each obj.object_name is a path; we will fetch and parse JSON
			try:
				total += 1
				if dry_run:
					logger.info(f"[dry-run] would process s3://{bucket}/{obj.object_name}")
					continue

				data_bytes = client.get_object(bucket, obj.object_name).read()
				payload = json.loads(data_bytes.decode('utf-8'))
				row = _extract_row_from_payload(payload)
				# By default we will upload processed CSV into MinIO processed-data/<city>/<date>.csv
				if upload_to_minio:
					success, reason = _append_row_to_minio_csv(row, client, bucket, prefix='processed-data', logger=logger)
					if success:
						written += 1
					else:
						logger.debug(f"Row not uploaded: {reason}")
					# Optionally also save a local copy if requested
					if save_local_copy:
						write_row_to_csv(row, output_base=output_base, logger=logger)
				else:
					out_file, ok = write_row_to_csv(row, output_base=output_base, logger=logger)
					if ok:
						written += 1
			except Exception as e:
				logger.error(f"Error processing {obj.object_name}: {e}")

	logger.info(f"Transform finished: total_objects_inspected={total}, rows_written={written}")
	return total, written


if AIRFLOW_AVAILABLE:
	default_args = {
		'owner': 'airflow',
		'depends_on_past': False,
		'start_date': days_ago(1),
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
	}

	dag = DAG(
		'transform_waqi_to_csv',
		default_args=default_args,
		description='Transform WAQI JSON from MinIO to per-city CSV in Processed-data',
		schedule_interval='@hourly',
		catchup=False,
		tags=['air-quality', 'transform', 'minio', 'processed-zone'],
	)


	def _run_transform():
		transform_minio_to_csv()


	transform_task = PythonOperator(
		task_id='transform_minio_to_csv',
		python_callable=_run_transform,
		dag=dag,
	)


def _parse_args():
	p = argparse.ArgumentParser()
	p.add_argument('--minio-host', default=MINIO_HOST)
	p.add_argument('--minio-access', default=MINIO_ACCESS)
	p.add_argument('--minio-secret', default=MINIO_SECRET)
	p.add_argument('--bucket', default=BUCKET)
	p.add_argument('--prefix', default=PREFIX)
	p.add_argument('--output-dir', default=BASE_OUTPUT_DIR)
	p.add_argument('--dry-run', action='store_true')
	p.add_argument('--upload-to-minio', dest='upload_to_minio', action='store_true', help='Upload processed CSVs into MinIO processed-data/ prefix (default)')
	p.add_argument('--no-upload', dest='upload_to_minio', action='store_false', help="Don't upload to MinIO; write locally instead")
	p.add_argument('--save-local', dest='save_local', action='store_true', help='Also save a local copy under dags/Processed-data in addition to uploading')
	return p.parse_args()


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
	args = _parse_args()
	transform_minio_to_csv(minio_host=args.minio_host, access=args.minio_access, secret=args.minio_secret,
						   bucket=args.bucket, prefix=args.prefix, output_base=args.output_dir,
						   dry_run=args.dry_run, upload_to_minio=args.upload_to_minio, save_local_copy=args.save_local, logger=logging.getLogger('transform'))

