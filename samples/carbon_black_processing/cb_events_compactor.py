import logging
from os import getenv
from botocore.config import Config
import boto3
from datetime import datetime, timedelta, timezone, UTC
import pyarrow.parquet as pq
import pyarrow as pa
import io

# -------- LOGGING -------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -------- CONFIGURATION -------- #
CB_EVENTS_S3_BUCKET_NAME = getenv("CB_EVENTS_S3_BUCKET_NAME")
OUTPUT_PREFIX = getenv("OUTPUT_PREFIX", "source=carbon_black_events_processed/")
TIME_WINDOW_DAYS = int(getenv("TIME_WINDOW_DAYS", 30))
MAX_UNCOMPRESSED_BYTES = 200 * 1024 * 1024  # 200MB

# -------- boto3 -------- #
botocore_retry_config = Config(
    retries={
        "max_attempts": 15,
        "mode": "adaptive"
    }
)

s3 = boto3.client("s3", config=botocore_retry_config)

def list_hourly_partition_prefixes(bucket: str, root_prefix: str, time_window_days: int = TIME_WINDOW_DAYS):
    """List hourly partition prefixes from time window defined in TIME_WINDOW_DAYS."""
    prefixes = set()
    threshold = datetime.now(UTC) - timedelta(days=int(time_window_days))

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=root_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet.zstd"):
                continue

            parts = key.split("/")
            try:
                year = int(parts[1].split("=")[1])
                month = int(parts[2].split("=")[1])
                day = int(parts[3].split("=")[1])
                hour = int(parts[4].split("=")[1])
                partition_dt = datetime(year, month, day, hour, tzinfo=timezone.utc)
            except (IndexError, ValueError) as e:
                logger.warning("Skipping malformed key: %s (%s)", key, e)
                continue

            if partition_dt >= threshold:
                hourly_prefix = "/".join(parts[:5]) + "/"  # Keep to hour-level prefix
                prefixes.add(hourly_prefix)

    return sorted(prefixes)

def list_parquet_files(bucket: str, prefix: str):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet.zstd"):
                keys.append(obj["Key"])
    return sorted(keys)

def download_parquet_to_table(bucket: str, key: str):
    response = s3.get_object(Bucket=bucket, Key=key)
    return pq.read_table(source=io.BytesIO(response["Body"].read()))

def align_tables_to_superset(tables: list[pa.Table]) -> list[pa.Table]:
    all_fields = {}
    for table in tables:
        for field in table.schema:
            all_fields[field.name] = field.type

    # Alphabetical order of fields
    sorted_fields = sorted(all_fields.items(), key=lambda x: x[0])
    superset_schema = pa.schema(sorted_fields)

    aligned_tables = []
    for table in tables:
        columns = {}
        for field in superset_schema:
            if field.name in table.column_names:
                columns[field.name] = table[field.name]
            else:
                columns[field.name] = pa.nulls(table.num_rows, type=field.type)
        aligned_table = pa.table(columns, schema=superset_schema)
        aligned_tables.append(aligned_table)

    return aligned_tables

def write_compacted_table(bucket: str, prefix: str, table: pa.Table, index: int):
    out_buffer = io.BytesIO()
    pq.write_table(table, out_buffer, compression="zstd")
    compacted_key = f"{prefix}compacted-part-{index:03}.parquet.zstd"
    s3.put_object(Bucket=bucket, Key=compacted_key, Body=out_buffer.getvalue())
    logger.info(
        "Wrote %s with %s rows",
        compacted_key, table.num_rows
    )

def delete_keys(bucket: str, keys: list):
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})

    logger.info(
        "Deleted %s old files",
        len(keys)
    )

def compact_partition(bucket: str, prefix: str):
    keys = list_parquet_files(bucket, prefix)
    if len(keys) < 2:
        return

    batch_index = 0
    current_tables = []
    current_bytes = 0
    all_keys = []

    for key in keys:
        try:
            table = download_parquet_to_table(bucket, key)
            current_tables.append(table)
            current_bytes += table.nbytes
            all_keys.append(key)
        except Exception as err:
            logger.error(
                "Failed to read %s: %s",
                key, err
            )
            continue

        if current_bytes >= MAX_UNCOMPRESSED_BYTES:
            try:
                aligned = align_tables_to_superset(current_tables)
                combined = pa.concat_tables(aligned)
                write_compacted_table(bucket, prefix, combined, batch_index)
                batch_index += 1
            except Exception as err:
                logger.error(
                    "Failed to combine batch in %s: %s",
                    prefix, err
                )
            current_tables = []
            current_bytes = 0

    if current_tables:
        try:
            aligned = align_tables_to_superset(current_tables)
            combined = pa.concat_tables(aligned)
            write_compacted_table(bucket, prefix, combined, batch_index)
        except Exception as err:
            logger.error(
                "Failed to write final batch in %s: %s",
                prefix, err
            )

    delete_keys(bucket, keys)

def main():
    hourly_prefixes = list_hourly_partition_prefixes(CB_EVENTS_S3_BUCKET_NAME, OUTPUT_PREFIX)

    logger.info("Found %s hourly partitions.", len(hourly_prefixes))
    for prefix in hourly_prefixes:
        compact_partition(CB_EVENTS_S3_BUCKET_NAME, prefix)

if __name__ == "__main__":
    main()
