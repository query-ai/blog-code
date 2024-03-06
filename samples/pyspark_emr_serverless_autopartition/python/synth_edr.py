from sys import argv
from faker import Faker
from typing import Tuple, NamedTuple
import random
import datetime
import ipaddress
import json
import uuid
import string
import pandas as pd

batchesArg = int(argv[1])
recordsArg = int(argv[2])

print(f"Creating {batchesArg} batches of {recordsArg} synthetic EDR entries.")

fake = Faker()

FAKENAMES = []
while len(FAKENAMES) < 6000:
    fakeName = str(fake.name())
    if "." in fakeName:
        continue
    if fakeName not in FAKENAMES:
        FAKENAMES.append(fake.name())

del fake

class SyntheticEdrDataPrep(NamedTuple):
    """
    Lists of benign & malware files, randomized hosts and file paths
    """
    fileSamples: list
    baseEdrHostData: list
    linuxFilepaths: list
    windowsFilePaths: list

def generateSyntheticTimestampNtz() -> Tuple[int, str]:
    """
    Generates various random timestamps and datetimes
    """
    randTs = random.randint(
        int(datetime.datetime(2023, 6, 10).timestamp()),
        int(datetime.datetime.now().timestamp())
    )

    randDate = datetime.datetime.fromtimestamp(randTs).replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999))
    
    randDateTs = int(str(randDate.timestamp()).split(".")[0])
    randDateTimestamp3 = str(randDate.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    #randDateTimestampNtz = str(randDate.strftime("%Y-%m-%d %H:%M:%S"))

    # Unix time and Snowflake TIMESTAMP_NTZ
    return (
        randDateTs,
        randDateTimestamp3
    )

def generateSyntheticMacAddress(numberOfMacs: int) -> list:
    """Generate n unique MAC addresses."""
    return [":".join(["%02x" % random.randint(0, 255) for _ in range(6)]) for _ in range(numberOfMacs)]

def generateSyntheticUuids(numberOfUuids: int) -> list:
    """
    Generates UUIDs used for device or agent UUIDs and returns a unique list
    """
    uuids = []

    while len(uuids) != numberOfUuids:
        newUuid = str(uuid.uuid4())
        if newUuid not in uuids:
            uuids.append(newUuid)

    return uuids

def generateSyntheticSensor(numberOfSensorIds: int, id_length=22) -> list:
    """
    Generate a list of unique sensor IDs.
    """
    sensorIds = set()
    
    while len(sensorIds) < numberOfSensorIds:
        # Generate the numeric part of the ID
        ipPart = ''.join(random.choices(string.digits, k=id_length))
        # Combine with the prefix and add to the set
        sensorId = f"sensor-{ipPart}"
        sensorIds.add(sensorId)
    
    return list(sensorIds)

def generateSyntheticRfc1918IpAddress() -> str:
    """
    Generates a random IP address within a randomly selected CIDR range.
    """
    network = ipaddress.IPv4Network(
        random.choice(
            [
                "10.100.0.0/16","10.0.0.0/16","192.168.0.0/16","172.16.0.0/16"
            ]
        ),
        strict=False
    )
    # subtract 2 to exclude the network and broadcast addresses
    randomPrivateIp = ipaddress.IPv4Address(network.network_address + random.randint(1, network.num_addresses - 2))

    return str(randomPrivateIp)

def generateSyntheticHostname(platformType: str) -> str:
    """
    Generates a synthetic Linux or Windows hostname based on a platform
    """
    if platformType == "Linux":
        distribution = random.choice(
            ["debian","rhel","centos","ubuntu","rocky","suse"]
        )
        role = random.choice(
            ["web","db","app","store","ftp","gateway"]
        )
        environment = random.choice(
            ["prod","dev","test","stage"]
        )
        identifier = random.randint(1,250)

        return f"{distribution}-{role}-{environment}-{identifier:03d}"

    if platformType == "Windows":
        prefix = random.choice([
            "WS","LAP","SRV","DESKTOP","MYCOMPUTER","WORKSTATION","ASSETNO","EWR17","ABE10","LAX8","LAX9","CHI5","CHI10","ATL22"
        ])
        identifier = random.randint(1,250)

        return f"{prefix}-{identifier:03d}"

def generateSyntheticFilepath(platformType: str) -> str:
    """
    Returns a path at random between typical malicious persistence and mostly-benign directories
    """
    if platformType == "Linux":
        persistencePaths = [
            "/etc/init.d/",  # Scripts executed during startup
            "/usr/local/bin/",  # Common place for executables
            "/usr/local/sbin/",  # System executables
            "/root/.bashrc",  # Executed by bash for root on startup
            "/home/user/.bash_profile",  # Executed by bash for users on login
            "/etc/cron.d/",  # Directory for cron jobs
            "/etc/cron.daily/",  # Daily cron jobs
            "/etc/cron.hourly/",  # Hourly cron jobs
            "/etc/rc.local",  # End of each multiuser runlevel
            "/var/spool/cron/crontabs/root/",  # Root crontab
            "/etc/ld.so.preload/",  # Preloaded libraries
            "/usr/share/initramfs-tools/hooks/",  # Initramfs tools
            "/etc/systemd/system/",  # Systemd service units
            "/lib/systemd/system/",  # Systemd system service units
        ]

        benignPaths = [
            "/home/user/Documents/",  # Document storage
            "/var/log/",  # Log files
            "/tmp/",  # Temporary files
            "/etc/passwd/",  # User account information
            "/usr/bin/",  # Executable files
            "/usr/share/",  # Architecture-independent data
            "/bin/",  # Essential command binaries
            "/lib/",  # Essential shared libraries
            "/opt/",  # Add-on application software packages
            "/etc/ssh/ssh_config/",  # SSH client configuration
            "~/home/",
            "~/",
            "~/sbin/",
            "~/var/options/",
            "~/tmp/",
            "/desktop/downloads",
            "~/Desktop/Downloads/",
            "~/Desktop/Documents/",
            "~/Desktop/Music/"
        ]

        return random.choice(persistencePaths + benignPaths)
    
    if platformType == "Windows":
        persistencePaths = [
            "C:\\Windows\\system32\\",  # System utilities and applications
            "C:\\Windows\\SysWOW64\\",  # 32-bit files on 64-bit installations
            "C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\",  # Startup programs
            "HKLM\\Software\\Microsoft\\Windows\\CurrentVersion\\Run\\",  # Registry Run key for all users
            "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Run\\",  # Registry Run key for current user
            "C:\\Users\\Administrator\\AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\Startup\\",  # User-specific startup
            "C:\\Windows\\Tasks\\",  # Task Scheduler tasks
            "C:\\Windows\\Prefetch\\",  # Prefetch files
            "HKLM\\Software\\Microsoft\\Windows NT\\CurrentVersion\\Windows\\AppInit_DLLs\\",  # DLLs loaded by processes
            "C:\\Windows\\system32\\drivers\\etc\hosts\\",
            "C:\\Windows\\system32\\drivers\\etc\\networks\\",
            "C:\\Windows\\system32\\config\\SAM\\",
            "C:\\Windows\\system32\\config\\SECURITY\\",
            "C:\\Windows\\system32\\config\\SOFTWARE\\",
            "C:\\Windows\\system32\\config\\SOFTWARE\\",
            "C:\\Windows\\system32\\config\\winevt\\",
            "C:\\Windows\\repair\\SAM\\",
            "C:\\Documents and Settings\\User\\Start Menu\\Programs\\Startup\\",
            "C:\\Windows\\Prefetch\\"
        ]

        benignPaths = [
            "C:\\Users\\Administrator\\Documents\\",  # User documents
            "C:\\Program Files\\",  # Installed applications (64-bit or 32-bit on 32-bit installations)
            "C:\\Program Files (x86)\\",  # Installed applications (32-bit on 64-bit installations)
            "C:\\Windows\\",  # Windows system directory
            "C:\\Users\\Administrator\\Downloads\\",  # Default download directory
            "C:\\Windows\\System32\\drivers\\",  # Device driver files
            "C:\\Temp\\",  # Temporary files
            "C:\\Users\\Administrator\\Desktop\\",  # User desktop
        ]

        return random.choice(persistencePaths + benignPaths)

def generateSyntheticUsername(platformType: str) -> str:
    """
    Generates synthetic usernames for Windows or Linux platforms
    """
    
    if platformType == "Linux":
        faked = str(random.choice(FAKENAMES)).lower()
        truncFirst = str(faked.split(" ")[0])[:4]
        truncLast = str(faked.split(" ")[1])[:4]
        commonSysNames = ["daemon","bin","sys","nobody","root"]
        commonSysNames.append(f"{truncFirst}{truncLast}")

        return random.choice(commonSysNames)
    
    if platformType == "Windows":
        faked = str(random.choice(FAKENAMES)).upper()
        truncFirst = str(faked.split(" ")[0])[:6]
        truncLast = str(faked.split(" ")[1])[:8]
        commonSysNames = ["Administrator","Guest","SYSTEM"]
        commonSysNames.append(f"{truncFirst}.{truncLast}")

        return random.choice(commonSysNames)

def stageSyntheticData() -> SyntheticEdrDataPrep:
    """
    Pre-stages base EDR host data, file paths, and file information for final generation
    """
    macAddresses = generateSyntheticMacAddress(numberOfMacs=6000)
    uuids = generateSyntheticUuids(numberOfUuids=6000)
    agentIds = generateSyntheticSensor(numberOfSensorIds=6000)
    ipAddresses = [generateSyntheticRfc1918IpAddress() for _ in range(6000)]
    linuxHosts = [generateSyntheticHostname(platformType="Linux") for _ in range(6000)]
    winHosts = [generateSyntheticHostname(platformType="Windows") for _ in range(6000)]
    linuxUsers = [generateSyntheticUsername(platformType="Linux") for _ in range(6000)]
    winUsers = [generateSyntheticUsername(platformType="Windows") for _ in range(6000)]

    print("Generated synthetic host data")

    malwareSamples = []
    benignSamples = []
    
    with open("./malware_samples.json", "r") as malwareread:
        for x in json.load(malwareread):
            x["severity_level"] = random.choice(["Malicious", "Suspicious"])
            malwareSamples.append(x)
    
    with open("./malware_samples.json", "r") as malwareread:
        for x in json.load(malwareread):
            x["severity_level"] = random.choice(["Benign", "Suspicious", "Exception"])
            benignSamples.append(x)

    fileSamples = malwareSamples + benignSamples
    del malwareSamples
    del benignSamples

    print("Generated file samples data")

    linuxEdrPayload = []
    while len(linuxEdrPayload) < 500:
        macAddress = str(random.choice(macAddresses))
        deviceId = str(random.choice(uuids))
        agentId = str(random.choice(agentIds))
        ipv4Address = str(random.choice(ipAddresses))
        hostname = str(random.choice(linuxHosts))
        username = str(random.choice(linuxUsers))

        payload = {
            "agent_id": agentId,
            "device_id": deviceId,
            "os_platform": "Linux",
            "computername": hostname,
            "ipv4_address": ipv4Address,
            "mac_address": macAddress,
            "username": username
        }
        linuxEdrPayload.append(payload)

    winEdrPayload = []
    while len(winEdrPayload) < 500:
        macAddress = str(random.choice(macAddresses))
        deviceId = str(random.choice(uuids))
        agentId = str(random.choice(agentIds))
        ipv4Address = str(random.choice(ipAddresses))
        hostname = str(random.choice(winHosts))
        username = str(random.choice(winUsers))

        payload = {
            "agent_id": agentId,
            "device_id": deviceId,
            "os_platform": "Linux",
            "computername": hostname,
            "ipv4_address": ipv4Address,
            "mac_address": macAddress,
            "username": username
        }
        winEdrPayload.append(payload)

    print("Generated edr payloads")
    
    # Combine payloads, and return the final list of items
    linuxFilePaths = [generateSyntheticFilepath(platformType="Linux") for _ in range(890)]
    winFilePaths = [generateSyntheticFilepath(platformType="Windows") for _ in range(890)]
    agentHostInfo = linuxEdrPayload + winEdrPayload

    return SyntheticEdrDataPrep(
        fileSamples=fileSamples,
        baseEdrHostData=agentHostInfo,
        linuxFilepaths=linuxFilePaths,
        windowsFilePaths=winFilePaths
    )
    
def finalRecord(numRecords: int) -> list:
    """
    first_seen_at, agent_id, device_id, os_platform, computername, ipv4_address, mac_address, username, filename, md5_hash, sha256_hash, severity_level, message

    """
    simData = stageSyntheticData()

    fileSamples = simData[0]
    agentHostInfo = simData[1]
    linuxFilePaths = simData[2]
    winFilePaths = simData[3]

    payloads = []
    while len(payloads) < numRecords:

        edrBase = random.choice(agentHostInfo)
        if edrBase["os_platform"] == "Linux":
            fileSample = random.choice(fileSamples)
            sampleName = str(fileSample["Filename"])
            path = str(random.choice(linuxFilePaths))
            filename = f"{path}{sampleName}"
            sevLevel = str(fileSample["severity_level"])
            msg = f"Potential {sevLevel.lower()} file found at {filename}"

            payload = {
                "first_seen_at": generateSyntheticTimestampNtz()[0],
                "agent_id": edrBase["agent_id"],
                "device_id": edrBase["device_id"],
                "os_platform": edrBase["os_platform"],
                "computername": edrBase["computername"],
                "ipv4_address": edrBase["ipv4_address"],
                "mac_address": edrBase["mac_address"],
                "username": edrBase["username"],
                "filename": filename,
                "md5_hash": fileSample["MD5"],
                "sha256_hash": fileSample["SHA256"],
                "severity_level": sevLevel,
                "message": msg
            }
            payloads.append(payload)
        elif edrBase["os_platform"] == "Windows":
            fileSample = random.choice(fileSamples)
            sampleName = str(fileSample["Filename"])
            path = str(random.choice(winFilePaths))
            filename = f"{path}{sampleName}"
            sevLevel = str(fileSample["severity_level"])
            msg = f"Potential {sevLevel.lower()} file found at {filename}"

            payload = {
                "first_seen_at": generateSyntheticTimestampNtz()[0],
                "agent_id": edrBase["agent_id"],
                "device_id": edrBase["device_id"],
                "os_platform": edrBase["os_platform"],
                "computername": edrBase["computername"],
                "ipv4_address": edrBase["ipv4_address"],
                "mac_address": edrBase["mac_address"],
                "username": edrBase["username"],
                "filename": filename,
                "md5_hash": fileSample["MD5"],
                "sha256_hash": fileSample["SHA256"],
                "severity_level": sevLevel,
                "message": msg
            }
            payloads.append(payload)
        else:
            continue

    return payloads

def generateFileUniqueId(length=12):
    """Generate a random string of fixed length."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

batchSize = batchesArg

for i in range(batchSize):
    # Generate DataFrame
    df = pd.DataFrame(finalRecord(numRecords=recordsArg))

    filename = f"./edr_samples/synthetic_edr_{generateFileUniqueId()}.parquet.zstd"

    df.to_parquet(
        path=filename,
        engine="auto",
        compression="zstd"
    )
    print(f"File saved: {filename}")