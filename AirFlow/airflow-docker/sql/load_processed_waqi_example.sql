-- DDL for processed WAQI CSV (one row per measurement)
-- Adjust types and table/schema names for your target database (Postgres, Redshift, Snowflake, etc.)

CREATE TABLE IF NOT EXISTS processed_waqi (
	city_label TEXT,
	city_name TEXT,
	measured_at TIMESTAMP WITH TIME ZONE,
	aqi INTEGER,
	dominentpol TEXT,
	idx INTEGER,
	lat DOUBLE PRECISION,
	lon DOUBLE PRECISION,
	co DOUBLE PRECISION,
	pm25 DOUBLE PRECISION,
	pm10 DOUBLE PRECISION,
	t DOUBLE PRECISION,
	h DOUBLE PRECISION,
	o3 DOUBLE PRECISION,
	p DOUBLE PRECISION,
	w DOUBLE PRECISION,
	dew DOUBLE PRECISION
);

-- Example: load CSV files produced under dags/Processed-data
-- Path in repo: dags/Processed-data/<city>/<YYYY-MM-DD>.csv
-- Replace <local_path> with a path accessible by your DB loader (or stage files in object storage)

-- Postgres (using psql + file access):
-- COPY processed_waqi FROM '/absolute/path/to/AirFlow/airflow-docker/dags/Processed-data/<city>/<YYYY-MM-DD>.csv' WITH (FORMAT csv, HEADER true);

-- If you use an object storage COPY (e.g. Redshift Spectrum, Snowflake, BigQuery) you can point to the MinIO/S3-compatible path
-- Example (conceptual):
-- COPY processed_waqi FROM 's3://air-quality/processed-data/<city>/<YYYY-MM-DD>.csv' CREDENTIALS 'aws_access_key_id=...;aws_secret_access_key=...' CSV HEADER;

-- NOTE: adapt the COPY/LOAD statement to your warehouse. The CSV schema follows the columns defined above.
