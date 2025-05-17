import logging
from os import getenv
from botocore.config import Config
import boto3
import gzip
import json
import io
import random
import string
import pyarrow as pa
import pyarrow.parquet as pq
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from multiprocessing import Pool, cpu_count
from typing import List
import argparse

# -------- LOGGING -------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -------- CONFIGURATION -------- #
ORG_KEY: str = getenv("ORG_KEY")

CB_EVENTS_S3_BUCKET_NAME: str = getenv("CB_EVENTS_S3_BUCKET_NAME")
S3_OUTPUT_BUCKET = CB_EVENTS_S3_BUCKET_NAME

INPUT_PREFIX: str = f"{getenv("INPUT_PREFIX")}/org_key={ORG_KEY}/"
OUTPUT_PREFIX: str = "source=carbon_black_events_processed/"

FILES_PER_BATCH = int(getenv("FILES_PER_BATCH", 500))
MAX_RECORDS = int(getenv("MAX_RECORDS", 100_000))
MAX_BYTES = 64 * 1024 * 1024

TIME_WINDOW_MINUTES = int(getenv("TIME_WINDOW_MINUTES", 30))

# -------- boto3 -------- #
botocore_retry_config = Config(
    retries={
        "max_attempts": 15,
        "mode": "adaptive"
    }
)

s3 = boto3.client("s3", config=botocore_retry_config)

def list_recent_s3_keys(prefix: str, window_minutes: int = TIME_WINDOW_MINUTES) -> List[str]:
    keys: List[str] = []

    now = datetime.now(timezone.utc)
    threshold = now - timedelta(minutes=int(window_minutes))

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=CB_EVENTS_S3_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl.gz") and obj["LastModified"] >= threshold:
                keys.append(obj["Key"])
    return keys

def random_suffix(length=28):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def parse_and_flatten_jsonl(content: bytes):
    records = []
    with gzip.GzipFile(fileobj=io.BytesIO(content), mode="rb") as gz:
        for line in gz:
            try:
                records.append(json.loads(line))
            except Exception:
                continue
    return records

def convert_timestamps(records):
    for rec in records:
        for ts_key in ("backend_timestamp", "device_timestamp"):
            val = rec.get(ts_key)
            if isinstance(val, str):
                try:
                    dt = datetime.strptime(val.split(" +")[0], "%Y-%m-%d %H:%M:%S")
                    rec[ts_key] = dt
                except Exception:
                    rec[ts_key] = None
            elif isinstance(val, (int, float)):
                try:
                    rec[ts_key] = datetime.fromtimestamp(val, tz=timezone.utc)
                except Exception:
                    rec[ts_key] = None
    return records

def determine_partition_path(base_prefix: str, timestamp: datetime) -> str:
    return (
        f"{base_prefix}"
        f"year={timestamp.year}/month={timestamp.month}/day={timestamp.day}/hour={timestamp.hour}/"
    )

def process_file_batch(batch_keys: List[str], delete_after: bool = False):
    partitioned_records = defaultdict(list)
    total_bytes = 0
    processed_keys = []

    for key in batch_keys:
        obj = s3.get_object(Bucket=CB_EVENTS_S3_BUCKET_NAME, Key=key)
        body = obj["Body"].read()
        total_bytes += len(body)

        records = parse_and_flatten_jsonl(body)
        records = convert_timestamps(records)

        for rec in records:
            ts = rec.get("backend_timestamp")
            if isinstance(ts, datetime):
                # Round down to the hour
                hour_key = datetime(ts.year, ts.month, ts.day, ts.hour)
                partitioned_records[hour_key].append(rec)

        processed_keys.append(key)

        if len(processed_keys) >= int(MAX_RECORDS) or total_bytes >= MAX_BYTES:
            break

    if not partitioned_records:
        logger.warning("No partitionable records found.")
        return

    for partition_hour, records in partitioned_records.items():
        try:
            table = pa.Table.from_pylist(records)

            for col in ("backend_timestamp", "device_timestamp"):
                if col in table.column_names:
                    table = table.set_column(
                        table.schema.get_field_index(col),
                        col,
                        table[col].cast(pa.timestamp("us"))
                    )

            partition_path = determine_partition_path(OUTPUT_PREFIX, partition_hour)
            filename = f"{partition_path}part-{random_suffix()}.parquet.zstd"

            out_buffer = io.BytesIO()
            pq.write_table(
                table,
                out_buffer,
                compression="zstd",
                use_deprecated_int96_timestamps=False,
                coerce_timestamps="us"
            )

            s3.put_object(
                Bucket=S3_OUTPUT_BUCKET,
                Key=filename,
                Body=out_buffer.getvalue()
            )
            logger.info("Wrote %s records to %s", len(records), filename)
        except Exception as e:
            logger.error("Failed to write partition %s: %s", partition_hour, e)

    if delete_after:
        for key in processed_keys:
            try:
                s3.delete_object(Bucket=CB_EVENTS_S3_BUCKET_NAME, Key=key)
                logger.info("Deleted %s after processing", key)
            except Exception as e:
                logger.warning("Failed to delete %s: %s", key, e)

def batch_process(keys: List[str], delete_after: bool = False, batch_size: int = FILES_PER_BATCH):
    batches = [keys[i:i + int(batch_size)] for i in range(0, len(keys), int(batch_size))]
    with Pool(cpu_count()) as pool:
        pool.starmap(process_file_batch, [(batch, delete_after) for batch in batches])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", action="store_true", help="Delete files after processing")
    args = parser.parse_args()

    keys = list_recent_s3_keys(INPUT_PREFIX)

    logger.info(
        "Discovered %s files from the last %s minutes.",
        len(keys), TIME_WINDOW_MINUTES    
    )

    if keys:
        batch_process(keys, delete_after=args.delete)
    else:
        logger.info(
            "No files were found in the last %s minutes to process",
            TIME_WINDOW_MINUTES
        )

# EOF