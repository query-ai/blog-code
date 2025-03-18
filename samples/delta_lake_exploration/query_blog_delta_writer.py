import numpy as np
import polars as pl
import ipaddress
import datetime
from hashlib import sha256
from deltalake import write_deltalake

S3_BUCKET_NAME = ""
S3_DELTA_PATH = f"s3://{S3_BUCKET_NAME}/deltalake/source=query_blog_lakehouse"
S3_BUCKET_LOCATION = ""
TOTAL_RECORDS = 250_000

INTERNAL_CIDRS = ["10.100.0.0/16", "192.168.1.0/16"]
CLIENT_CIDRS = ["47.16.64.0/20", "12.88.80.0/20", "94.126.208.0/21"]

INTERNAL_PORTS = np.array([80, 443, 22, 3389, 445, 21, 120, 1521], dtype=np.int16)
CLIENT_PORTS = (3200, 62000)

DIRECTIONS = np.array(["in", "out"], dtype=object)
ACTIONS = np.array(["allow", "drop", "block", "inspect"], dtype=object)

TOTAL_BYTES_RANGE = (32, 69420)
TOTAL_PACKETS_RANGE = (8, 4209)

def randomIpFromCidr(cidr: str) -> str:
    """Generates an IP based on a CIDR/Subnet Mask"""
    network = ipaddress.ip_network(cidr, strict=False)
    return str(network.network_address + np.random.randint(1, network.num_addresses - 1))

def generatePrivateIps(n: int) -> np.ndarray:
    """Generate all internal/private IPs"""
    return np.array([randomIpFromCidr(np.random.choice(INTERNAL_CIDRS)) for _ in range(n)])

def generatePublicIps(n: int) -> np.ndarray:
    """Generate all client/public IPs"""
    return np.array([randomIpFromCidr(np.random.choice(CLIENT_CIDRS)) for _ in range(n)])

def generateSyntheticTimestamps(n: int) -> np.ndarray:
    """Generates timestamps between now and 5 days ago"""
    now = datetime.datetime.now()
    past = now - datetime.timedelta(days=5)
    timestamps = past.timestamp() + np.random.randint(0, int((now - past).total_seconds()), size=n)
    return np.array([datetime.datetime.fromtimestamp(ts).isoformat() for ts in timestamps])

def generateEventIds(privateIps: np.ndarray, publicIps: np.ndarray, timestamps: np.ndarray) -> np.ndarray:
    """Generates an event ID using a hash of the timestamp, public/client, and private/internal IPs"""
    return np.array([
        sha256(f"{pIp}{cIp}{ts}".encode()).hexdigest()
        for pIp, cIp, ts in zip(privateIps, publicIps, timestamps)
    ])

def generateSyntheticNetworkLogs(totalRecords: int) -> pl.DataFrame:
    """Assemble the synthetic logs"""
    privateIps = generatePrivateIps(totalRecords)
    publicIps = generatePublicIps(totalRecords)
    timestamps = generateSyntheticTimestamps(totalRecords)
    eventIds = generateEventIds(privateIps, publicIps, timestamps)

    df = pl.DataFrame({
        "event_time": timestamps,
        "event_id": eventIds,
        "internal_ip": privateIps,
        "internal_port": np.random.choice(INTERNAL_PORTS, size=totalRecords),
        "client_ip": publicIps,
        "client_port": np.random.randint(*CLIENT_PORTS, size=totalRecords),
        "direction": np.random.choice(DIRECTIONS, size=totalRecords),
        "action": np.random.choice(ACTIONS, size=totalRecords),
        "total_bytes": np.random.randint(*TOTAL_BYTES_RANGE, size=totalRecords),
        "total_packets": np.random.randint(*TOTAL_PACKETS_RANGE, size=totalRecords),
    })

    df = df.with_columns(
        pl.col("event_time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S.%f", strict=False).alias("event_time")
    )

    df = df.with_columns(
        pl.col("event_time").dt.year().cast(pl.Int32).alias("year"),
        pl.col("event_time").dt.month().cast(pl.Int32).alias("month"),
        pl.col("event_time").dt.day().cast(pl.Int32).alias("day")
    )
    
    return df

def writeSyntheticNetworkLogsToDelta(totalRecords: int):
    """Sends synth network logs to Delta Lake in S3 using year/month/day partitions"""
    df = generateSyntheticNetworkLogs(totalRecords)

    print(df.head(n=5))
    
    write_deltalake(
        S3_DELTA_PATH,
        df,
        mode="append",
        partition_by=["year", "month", "day"],
        storage_options={"AWS_REGION": S3_BUCKET_LOCATION},
    )
    print(f"Data written to Delta at {S3_DELTA_PATH}")

writeSyntheticNetworkLogsToDelta(TOTAL_RECORDS)