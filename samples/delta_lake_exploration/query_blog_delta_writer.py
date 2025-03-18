import random
import ipaddress
import datetime
from hashlib import sha256
import pandas as pd
from deltalake import write_deltalake

SS_BUCKET_NAME = ""
S3_DELTA_PATH = f"s3://{SS_BUCKET_NAME}/"
S3_BUCKET_LOCATION = ""

def randomIpFromCidr(cidr: str) -> str:
    network = ipaddress.IPv4Network(cidr, strict=False)
    return str(random.choice(list(network.hosts())))

def generatePrivateIp() -> str:
    return random.choice([
        randomIpFromCidr("10.100.0.0/16"),randomIpFromCidr("192.168.1.0/16")
    ])

def generatePublicIp() -> str:
    return random.choice([
        randomIpFromCidr("47.16.64.0/20"),randomIpFromCidr("12.88.80.0/20"),randomIpFromCidr("94.126.208.0/21")
    ])

def generateSyntheticTimestampNtz() -> str:
    """
    Generate a random Timestamp(3) between now and 12 Hours From Now
    """

    now = datetime.datetime.now()
    tweleveHoursFromNowLol = now + datetime.timedelta(hours=12)

    randTs = random.randint(
        int(now.timestamp()),
        int(tweleveHoursFromNowLol.timestamp())
    )

    # Generated data will have OO:OO:OO.00000, make it spicier
    randDate = datetime.datetime.fromtimestamp(randTs).replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999))
    
    randDateTimestampNtz = str(randDate.strftime("%Y-%m-%d %H:%M:%S.%f"))

    # TIMESTAMP(3) / TIMESTAMP_NTZ
    return randDateTimestampNtz

def generateHexdigest(privateIp: str, publicIp: str, timestamp: str) -> str:
    combinedString = f"{privateIp}{publicIp}{timestamp}"
    return sha256(combinedString.encode()).hexdigest()

def generateSyntheticNetworkLogs(totalRecords: int) -> list[dict]:
    """Generates random network logs"""
    syntheticLogs: list[dict] = []

    while len(syntheticLogs) != totalRecords:
        privateIp = generatePrivateIp()
        publicIp = generatePublicIp()
        eventTime = generateSyntheticTimestampNtz()

        eventId = generateHexdigest(privateIp,publicIp,eventTime)

        log = {
            "event_time": eventTime,
            "event_id": eventId,
            "internal_ip": privateIp,
            "internal_port": random.choice([80,443,22,3389,445,21,120,1521]),
            "client_ip": publicIp,
            "client_port": random.randint(3200, 62000),
            "direction": random.choice(["in","out"]),
            "action": random.choice(["allow","drop","block","inspect"]),
            "total_bytes": random.randint(32,69420),
            "total_packets": random.randint(8,4209)
        }

        syntheticLogs.append(log)

    return syntheticLogs

def writeSyntheticNetworkLogsToDelta(totalRecords: int):
    """Writes the DataFrame to a Delta Table on S3 with partitions."""
    df = pd.DataFrame(generateSyntheticNetworkLogs(totalRecords))

    df["event_time"] = pd.to_datetime(df["event_time"])
    df["year"] = df["event_time"].dt.year
    df["month"] = df["event_time"].dt.month
    df["day"] = df["event_time"].dt.day
    df["hour"] = df["event_time"].dt.hour

    write_deltalake(
        S3_DELTA_PATH, 
        df, 
        mode="append",
        partition_by=["year","month","day","hour"],
        storage_options={"AWS_REGION": S3_BUCKET_LOCATION}
    )
    print(f"Data written to Delta at {S3_DELTA_PATH}")

writeSyntheticNetworkLogsToDelta(totalRecords=100)