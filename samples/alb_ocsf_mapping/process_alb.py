import logging
from pygrok import Grok
from os import path, remove
from boto3 import client, resource
from gzip import open as gunzip
from datetime import datetime
from urllib.parse import urlparse
import json
import pandas as pd

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

GROK = Grok(
    '%{DATA:type}\s+%{TIMESTAMP_ISO8601:time}\s+%{DATA:elb}\s+%{DATA:client}\s+%{DATA:target}\s+%{BASE10NUM:request_processing_time}\s+%{DATA:target_processing_time}\s+%{BASE10NUM:response_processing_time}\s+%{BASE10NUM:elb_status_code}\s+%{DATA:target_status_code}\s+%{BASE10NUM:received_bytes}\s+%{BASE10NUM:sent_bytes}\s+\"%{DATA:request}\"\s+\"%{DATA:user_agent}\"\s+%{DATA:ssl_cipher}\s+%{DATA:ssl_protocol}\s+%{DATA:target_group_arn}\s+\"%{DATA:trace_id}\"\s+\"%{DATA:domain_name}\"\s+\"%{DATA:chosen_cert_arn}\"\s+%{DATA:matched_rule_priority}\s+%{TIMESTAMP_ISO8601:request_creation_time}\s+\"%{DATA:actions_executed}\"\s+\"%{DATA:redirect_url}\"\s+\"%{DATA:error_reason}\"\s+\"%{DATA:target_list}\"\s+\"%{DATA:target_status_code_list}\"\s+\"%{DATA:classification}\"\s+\"%{DATA:classification_reason}\"'
)

BUCKET_NAME = "alb-access-logs-query-se-sandbox"
PATH_NAME = "AWSLogs/125343585094/elasticloadbalancing/us-east-2/2024/11/17"

def openLogFile(bucket: str, prefix: str):
    """
    Downloads and parses all log files stored in S3 under a given prefix
    """
    ocsfLogs: list[dict] = []
    s3 = resource("s3")
    s3Client = client("s3")

    # List all objects under the given prefix (S3 path)
    objects = s3Client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    logger.info(f"Processing {len(objects)} Objects.")

    if 'Contents' not in objects:
        print(f"No files found in {bucket}/{prefix}")
        return

    # Iterate over all objects in the prefix
    for obj in objects['Contents']:
        key = obj['Key']
        filename = key.replace("/", "_")
        
        # Download each file
        s3.meta.client.download_file(bucket, key, f"/tmp/{filename}")
        try:
            # Uncompress and process the logs
            with gunzip(f"/tmp/{filename}", mode="rt") as logs:
                for rawlog in logs:
                    processed = grokProcessLogs(rawlog)
                    if processed:
                        ocsfLogs.append(grokProcessLogs(rawlog))
        finally:
            # Clean up the downloaded file
            if path.exists(f"/tmp/{filename}"):
                remove(f"/tmp/{filename}")

    df = pd.DataFrame(ocsfLogs)

    print(df.head(n=10))

    df.to_parquet("./awsalb_ocsf_http_activity.parquet.zstd",compression="zstd")

def grokProcessLogs(rawlog: str) -> dict | None:
    """
    Uses PyGrok to transform ALB access log pattern into Python dictionary and further OCSF conversion
    """
    preProcessedLog = GROK.match(rawlog)
    # ALB access log docs don't account for this, but if the TG ARN is empty it is likely a log delivery error and should be ignored
    try:
        if preProcessedLog["target_group_arn"] is not None and preProcessedLog["target_group_arn"] != "-":
            try:
                tgSplitter = preProcessedLog["target_group_arn"].split(":")
                preProcessedLog["region"] = tgSplitter[3]
                preProcessedLog["account"] = str(tgSplitter[4])

                baseEventMapping = httpActivityBaseEventMapping(preProcessedLog["request"].split(" ")[0])
                dstEndpoint = elbTargetProcessor(preProcessedLog["target"])

                ocsf = httpActivityOcsfBuilder(rawlog, preProcessedLog, baseEventMapping, dstEndpoint)
                return ocsf
            except IndexError:
                pass
    except TypeError:
        return None

def elbTargetProcessor(target: str) -> dict[str | None]:
    """
    Based on the "target" field ("target:port" in the ALB syntax) return an OCSF dst_endpoint object as Lambda functions
    will not return any information, nor will failed requests
    """
    if target != None:
        dstEndpoint = {
            "ip": None,
            "port": None
        }
    else:
        try:
            targetSplit = target.split(":")
            dstEndpoint = {
                "ip": targetSplit[0],
                "port": int(targetSplit[1])
            }
        except KeyError or IndexError:
            dstEndpoint = {
                "ip": None,
                "port": None
            }

    return dstEndpoint

def processUrlObject(urlString: str) -> dict[str | int | None]:
    """
    Uses urllib.parse to process the URL string contained within the Request portion of the ALB log
    """

    p = urlparse(urlString)

    try:
        port = p.port
    except ValueError:
        port = None

    return {
        "hostname": p.hostname or None,
        "path": p.path or None,
        "port": port,
        "query_string": p.query or None,
        "scheme": p.scheme or None
    }

def httpActivityBaseEventMapping(httpMethod: str) -> dict[str | int]:
    """
    For events that match HTTP Activity Event - map Base Event attributes such as class_name and type_uid and so forth
    """
    if httpMethod == "CONNECT":
        activityId = 1
        activityName = "Connect"
        typeUid = 400201
        typeName = "HTTP Activity: Connect"
    elif httpMethod == "DELETE":
        activityId = 2
        activityName = "Delete"
        typeUid = 400202
        typeName = "HTTP Activity: Delete"
    elif httpMethod == "GET":
        activityId = 3
        activityName = "Get"
        typeUid = 400203
        typeName = "HTTP Activity: Get"
    elif httpMethod == "HEAD":
        activityId = 4
        activityName = "Head"
        typeUid = 400204
        typeName = "HTTP Activity: Head"
    elif httpMethod == "OPTIONS":
        activityId = 5
        activityName = "Options"
        typeUid = 400205
        typeName = "HTTP Activity: Options"
    elif httpMethod == "POST":
        activityId = 6
        activityName = "Post"
        typeUid = 400206
        typeName = "HTTP Activity: Post"
    elif httpMethod == "PUT":
        activityId = 7
        activityName = "Put"
        typeUid = 400207
        typeName = "HTTP Activity: Put"
    elif httpMethod == "TRACE":
        activityId = 8
        activityName = "Trace"
        typeUid = 400208
        typeName = "HTTP Activity: Trace"
    else:
        activityId = 99
        activityName = "Other"
        typeUid = 400299
        typeName = "HTTP Activity: Other"

    return {
        "ActivityId": activityId,
        "ActivityName": activityName,
        "TypeUid": typeUid,
        "TypeName": typeName
    }

def ocsfStatusNormalization(httpStatusCode: str) -> dict[str | int]:
    """Transforms status code into normalize status"""
    if httpStatusCode.startswith("1") or httpStatusCode.startswith("2") or httpStatusCode.startswith("3"):
        status = "Success"
        statusId = 1
    else:
        status = "Failure"
        statusId = 2

    return {
        "Status": status,
        "StatusId": statusId
    }

def tlsNormalization(preProcessedLog: dict) -> dict[str | None]:
    """Normalizes TLS data from ALB"""

    tlsCipher = None if preProcessedLog["ssl_cipher"] == "-" else preProcessedLog["ssl_cipher"]
    tlsSni = None if preProcessedLog["domain_name"] == "-" else preProcessedLog["domain_name"]
    tlsVersion = None if preProcessedLog["ssl_protocol"] == "-" else preProcessedLog["ssl_protocol"]

    return {
        "TlsCipher": tlsCipher,
        "TlsSni": tlsSni,
        "TlsVersion": tlsVersion
    }

def convertIso8061ToSqlTimestamp(isoTimestamp: str):
    isoTimestamp = isoTimestamp.split(".")[0]
    # Parse the ISO 8061 timestamp
    dt = datetime.strptime(isoTimestamp, "%Y-%m-%dT%H:%M:%S")
    
    # Format it to the SQL TIMESTAMP(3) format (including milliseconds)
    return str(dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])

def httpActivityOcsfBuilder(rawlog: str, preProcessedLog: dict, baseEventMapping: dict[str | int], dstEndpoint: dict[str | None]) -> dict:
    """
    This function assembles the final OCSF schema structure for HTTP Activity
    """
    # Status Code provides status_code, status, and status_id
    statusCode = preProcessedLog["elb_status_code"]
    statusNormalization = ocsfStatusNormalization(statusCode)
    statusDetail = None if preProcessedLog["error_reason"] == "-" else preProcessedLog["error_reason"]

    # Actions executied -> message
    actionsExec = preProcessedLog["actions_executed"]
    message = f"ALB executed the following actions: {actionsExec}"

    # Client maps to src_endpoint
    clientSplit = preProcessedLog["client"].split(":")

    # Request maps to http_request
    requestSplit = preProcessedLog["request"].split(" ")

    # Parse URL info
    parsedUrl = processUrlObject(requestSplit[1])

    # Parse TLS information
    normalizedTls = tlsNormalization(preProcessedLog)

    # Add target group ARN to dst_endpoint.uid
    dstEndpoint["uid"] = preProcessedLog["target_group_arn"]

    # Change ISO 8601 datatimes into SQL-like TIMESTAMP_NTZ(3)
    eventTime = convertIso8061ToSqlTimestamp(preProcessedLog["time"])
    startTime = convertIso8061ToSqlTimestamp(preProcessedLog["request_creation_time"])

    # Create an ARN for the ELB, only a portion is in the raw log, add it to the src endpoint
    elbId = preProcessedLog["elb"]
    awsAccount = preProcessedLog["account"]
    awsRegion = preProcessedLog["region"]
    elbArn = f"arn:aws:elasticloadbalancing:{awsRegion}:{awsAccount}:loadbalancer/{elbId}"

    # Normalize Observables
    observables = [
        {
            "name": "src_endpoint.ip",
            "type": "IP Address",
            "type_id": 2,
            "value": clientSplit[0]
        },
        {
            "name": "src_endpoint.port",
            "type": "Port",
            "type_id": 11,
            "value": clientSplit[1]
        },
        {
            "name": "http_request.url.url_string",
            "type": "URL String",
            "type_id": 6,
            "value": requestSplit[1]
        },
        {
            "name": "src_endpoint.uid",
            "type": "Resource UID",
            "type_id": 10,
            "value": elbArn
        },
        {
            "name": "dst_endpoint.uid",
            "type": "Resource UID",
            "type_id": 10,
            "value": preProcessedLog["target_group_arn"]
        },
        {
            "name": "http_request.user_agent",
            "type": "User Agent",
            "type_id": 16,
            "value": preProcessedLog["user_agent"]
        },
        {
            "name": "cloud.account.uid",
            "type": "Account UID",
            "type_id": 35,
            "value": awsAccount
        }
    ]
    if dstEndpoint["ip"]:
        observables.append(
            {
                "name": "dst_endpoint.ip",
                "type": "IP Address",
                "type_id": 2,
                "value": dstEndpoint["ip"]
            }
        )
    if dstEndpoint["port"]:
        observables.append(
            {
                "name": "dst_endpoint.port",
                "type": "Port",
                "type_id": 11,
                "value": dstEndpoint["port"]
            }
        )

    ocsf = {
        "activity_id": baseEventMapping["ActivityId"],
        "activity_name": baseEventMapping["ActivityName"],
        "category_name": "Network Activity",
        "category_uid": 4,
        "class_name": "HTTP Activity",
        "class_uid": 4002,
        "severity_id": 1,
        "severity": "Informational",
        "status": statusNormalization["Status"],
        "status_code": statusCode,
        "status_detail": statusDetail,
        "status_id": statusNormalization["StatusId"],
        "type_uid": baseEventMapping["TypeUid"],
        "type_name": baseEventMapping["TypeName"],
        "message": message,
        "time": eventTime,
        "start_time": startTime,
        "duration": float(preProcessedLog["request_processing_time"]) + float(preProcessedLog["target_processing_time"]) + float(preProcessedLog["response_processing_time"]),
        "raw_data": rawlog,
        "metadata": {
            "uid": preProcessedLog["trace_id"],
            "logged_time": eventTime,
            "orignal_time": eventTime,
            "version": "1.4.0",
            "profiles": ["cloud"],
            "product": {
                "name": "Amazon Elastic Load Balancing",
                "vendor_name": "AWS",
                "feature": {
                    "name": "AlbAccessLogs"
                }
            }
        },
        "observables": observables,
        "cloud": {
            "account": {
                "type_id": 10,
                "type": "AWS Account",
                "uid": awsAccount
            },
            "region": awsRegion,
            "provider": "AWS"
        },
        "connection_info": {
            "boundary_id": 3,
            "boundary": "External",
            "direction_id": 1,
            "direction": "Inbound",
            "protocol_name": "tcp",
            "protocol_num": 6,
            "uid": preProcessedLog["trace_id"],
        },
        "dst_endpoint": dstEndpoint,
        "http_request": {
            "http_method": requestSplit[0].upper(),
            "version": requestSplit[2],
            "user_agent": preProcessedLog["user_agent"],
            "uid": preProcessedLog["trace_id"],
            "url": {
                "hostname": parsedUrl["hostname"],
                "path": parsedUrl["path"],
                "port": parsedUrl["port"],
                "query_string": parsedUrl["query_string"],
                "scheme": parsedUrl["scheme"],
                "url_string": requestSplit[1]
            }
        },
        "src_endpoint": {
            "ip": clientSplit[0],
            "port": int(clientSplit[1]),
            "uid": elbArn
        },
        "traffic": {
            "bytes_out": int(preProcessedLog["sent_bytes"]),
            "bytes_in": int(preProcessedLog["received_bytes"]),
            "bytes": int(preProcessedLog["sent_bytes"]) + int(preProcessedLog["received_bytes"])
        },
        "tls": {
            "cipher": normalizedTls["TlsCipher"],
            "sni": normalizedTls["TlsSni"],
            "version": normalizedTls["TlsVersion"],
        },
        "unmapped": {
            "target_status_code": preProcessedLog["target_status_code"],
            "chosen_cert_arn": preProcessedLog["chosen_cert_arn"],
            "matched_rule_priority": preProcessedLog["matched_rule_priority"],
            "redirect_url": preProcessedLog["redirect_url"],
            "target_list": preProcessedLog["target_list"],
            "target_status_code_list": preProcessedLog["target_status_code_list"],
            "classification": preProcessedLog["classification"],
            "classification_reason": preProcessedLog["classification_reason"],
        }
    }

    return ocsf

openLogFile(
    bucket=BUCKET_NAME,
    prefix=PATH_NAME
)

# eof