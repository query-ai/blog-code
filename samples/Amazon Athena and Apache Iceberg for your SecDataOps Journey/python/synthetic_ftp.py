from typing import NamedTuple, Tuple
import json
import random
import datetime

class FtpRespCodeAndMessage(NamedTuple):
    """
    Defines the FTP operation and the response code and message based on the operation
    """
    operation: str
    responseCode: int
    message: str

def generateSyntheticTimestampNtz() -> Tuple[int, str]:
    """
    Generates a random TIMESTAMP_NTZ formatted time and Epochseconds.
    """
    randTs = random.randint(
        int(datetime.datetime(2024, 1, 1).timestamp()),
        int(datetime.datetime.now().timestamp())
    )

    # Generated data will have OO:OO:OO
    randDate = datetime.datetime.fromtimestamp(randTs).replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
    )
    
    randDateTs = int(str(randDate.timestamp()).split(".")[0])
    randDateTimestampNtz = str(randDate.strftime("%Y-%m-%d %H:%M:%S.%f"))
    #randDateTimestampNtz = str(randDate.strftime("%Y-%m-%d %H:%M:%S"))

    # Unix time and Snowflake TIMESTAMP_NTZ
    return (
        randDateTs,
        randDateTimestampNtz
    )

def generateSyntheticFtpOperation() -> FtpRespCodeAndMessage:
    """
    Returns the Response Code and Message of a randomly chosen FTP operation
    """
    # Operations and possible outcomes (success, failure)
    operations = {
        'STOR': {
            'SUCCESS': (226, 'File upload successful. Closing data connection.'),
            'FAILURE': (550, 'Failed to upload file. File unavailable or access denied.')
        },
        'RETR': {
            'SUCCESS': (226, 'File download successful. Closing data connection.'),
            'FAILURE': (550, 'Failed to download file. File unavailable or access denied.')
        },
        'LIST': {
            'SUCCESS': (226, 'Directory listing successful.'),
            'FAILURE': (550, 'Failed to list directory. Directory not found.')
        },
        'DELE': {
            'SUCCESS': (250, 'File deletion successful.'),
            'FAILURE': (550, 'Failed to delete file. File not found.')
        },
        'MKD': {
            'SUCCESS': (257, 'Directory created successfully.'),
            'FAILURE': (550, 'Failed to create directory. Directory already exists.')
        },
        'RMD': {
            'SUCCESS': (250, 'Directory removed successfully.'),
            'FAILURE': (550, 'Failed to remove directory. Directory not found.')
        },
        'CWD': {
            'SUCCESS': (250, 'Changed working directory successfully.'),
            'FAILURE': (550, 'Failed to change directory. Directory not found.')
        }
    }

    operation = random.choice(list(operations.keys()))
    outcome = 'SUCCESS' if random.choice([True, False]) else 'FAILURE'
    respCode, message = operations[operation][outcome]

    return FtpRespCodeAndMessage(
        operation=operation,
        responseCode=respCode,
        message=message
    )

def generateSyntheticFtpPath() -> str:
    """
    Returns a synthetic path based on the Ubuntu filesystem and randomly generated names
    """
    ftpPaths = [
        "/var/www/upload-app",
        "/var/www/s3-gateway",
        "/var/www/datasync",
        "/var/www/html",
        "/var/vsftpd",
        "/var/vsftpd/priv",
        "/var/vsftpd/pub",
        "/srv/ftp",
        "/srv/files/ftp",
        "/home/user/ftp",
        "/home/ssm-user/ftp",
        "/home/sudo/ftp",
        "/home/ftp",
        "/home/webuser/public/widget.co/public",
        "/home/webuser/public/widget.co/secure",
        "/home/webuser/public/contoso.org/public",
        "/home/webuser/public/contoso.org/secure"
    ]

    fileNames = [
        "index","file","roadmap","ledger","emails","homepage","newsletter","board_minutes","financial_report","signups","config","conf","query","the_big_succ"
    ]

    fileMimes = [
        ".txt",".md",".html",".htm",".parquet",".json",".csv",".xslx",".pdf",".pptx",".tsv",".parquet.zstd",".json.gzip",".conf",".toml",".pkl"
    ]

    return f"{random.choice(ftpPaths)}/{random.choice(fileNames)}{random.choice(fileMimes)}"

def generateSyntheticFtpUsername() -> str:
    """
    Returns a randomly generated username
    """

    return random.choice(
        [
            "administrator","admin","oracle","mysql","psql","user","guest","ssm-user","apache","info","test","tester","root","sudo","main",f"user_{random.randint(1,100)}"
        ]
    )

def generateIpAddress() -> str:
    """
    Returns a synthetic or honeypot IP
    """
    ipAddresses = [
        "10.100.0.27","10.100.3.69","10.100.7.42","10.100.12.127","10.100.0.27","192.168.0.33","192.168.11.55","192.168.22.21","192.168.72.169","192.168.1.12","10.100.0.95", "10.100.0.129", "10.100.2.146", "10.100.0.27", "10.100.2.140","10.100.1.202", "10.100.0.71", "10.100.0.154", "10.100.1.136", "10.100.2.127","185.125.190.57", "64:ff9b:0:0:0:0:a64:292", "64:ff9b:0:0:0:0:a64:81","2600:1f16:127a:2000:c802:a75f:63f1:f9c9", "2a01:4ff:1f0:e553:0:0:0:1","64:ff9b:0:0:0:0:a64:188", "185.125.190.56", "64:ff9b:0:0:0:0:34db:6922","64:ff9b:0:0:0:0:34db:64f2", "193.35.18.33", "103.157.26.39", "192.241.239.40","64:ff9b:0:0:0:0:a64:23f", "10.100.2.63", "10.100.2.139", "185.254.196.238","10.100.0.44", "64:ff9b:0:0:0:0:34db:629a", "2600:1f16:127a:2002:8684:6997:d29a:d9ec","2600:1f18:4a3:6902:b95e:13c8:ea01:fda0", "64:ff9b:0:0:0:0:a64:5f", "10.100.0.177","64:ff9b:0:0:0:0:a64:197", "198.235.24.213", "2607:f1c0:1800:7b:0:0:0:1","37.221.173.241", "2600:1f18:4a3:6901:2f65:3de:4135:a6a4", "64:ff9b:0:0:0:0:34db:6b1a","205.210.31.3", "147.78.47.67", "5.78.89.3", "10.100.1.151", "10.100.1.226","2600:1f18:4a3:6902:6a8a:b72:c16e:db31", "192.241.195.49", "64:ff9b:0:0:0:0:a64:b1","198.235.24.73", "2600:1f18:4a3:6902:9df1:2d37:68ce:e611", "143.42.191.145","67.203.7.141","2600:1f18:4a3:6902:2a6f:3bec:e00b:8fb9", "143.42.76.146","46.174.191.31", "146.88.240.94", "2600:1f18:4a3:6901:f191:59e4:8a22:4973","2600:1f18:4a3:6900:5e97:61b4:accf:663e", "174.138.61.44", "184.105.247.204","107.170.245.5", "193.163.125.40", "10.100.1.5", "45.128.232.183", "162.243.142.47","185.216.71.6", "3.87.127.143", "46.101.66.191", "207.180.223.28", "129.151.243.99","52.219.94.154", "64:ff9b:0:0:0:0:34db:62f2", "89.187.164.129"
    ]

    return random.choice(ipAddresses)

def generateSyntheticFtpLog():
    syntheticRespCodeAndMsg = generateSyntheticFtpOperation()

    logEntry = {
        "unix_time": generateSyntheticTimestampNtz()[0],
        "operation": syntheticRespCodeAndMsg[0],
        "response_code": syntheticRespCodeAndMsg[1],
        "client_ip": generateIpAddress(),
        "file_path": generateSyntheticFtpPath(),
        "size": random.randint(1, 1024**2) if syntheticRespCodeAndMsg[0] in ['STOR', 'RETR'] else 0,
        "username": generateSyntheticFtpUsername(),
        "message": syntheticRespCodeAndMsg[2]
    }

    return logEntry

ftpLogs = []
while len(ftpLogs) < 15000:
    ftpLogs.append(generateSyntheticFtpLog())

with open("./synthetic_ftp_logs.json", "w") as jsonfile:
    json.dump(
        ftpLogs,
        jsonfile,
        indent=4
    )