# %%
import duckdb

LOCAL_JSON = "synthetic_edr_data_with_process.json"

# %%
# If you're unfamiliar with the dataset, the first action you can take is getting a COUNT of the total amount of rows
duckdb.sql(
    f"""
    SELECT COUNT(*) FROM read_json('{LOCAL_JSON}')
    """
).show()

# %%
# Most basic EDA - SELECT * - using a LIMIT will prevent overwhelming yourself
duckdb.sql(
    f"""
    SELECT * FROM read_json('{LOCAL_JSON}')
    LIMIT 10
    """
).show()

# %%
# To query for unique values, use SELECT DISTINCT
duckdb.sql(
    f"""
    SELECT DISTINCT * FROM read_json('{LOCAL_JSON}')
    LIMIT 10
    """
).show()

# %%
# SELECT DISTINCT is more impactful when specifying small numbers of fields, better to do so without a LIMIT, depending on the use case
duckdb.sql(
    f"""
    SELECT DISTINCT
        device.ip,
        file.name
    FROM read_json('{LOCAL_JSON}')
    LIMIT 10
    """
).show()

# %%
# When specifying nested fields with dot-notation, use aliASes to maintain read-friendly naming conventions
duckdb.sql(
    f"""
    SELECT DISTINCT
        device.ip AS device_ip,
        file.name AS file_name
    FROM read_json('{LOCAL_JSON}')
    LIMIT 10
    """
).show()

# %%
# Pairing the COUNT() function with SELECT DISTINCT allows you to find the total amount of distinct values. This can help an analyst make a decision with LIMIT or how many fields they want to process with SELECT DISTINCT at a given time. The more fiedls, the bigger the resulting result set. In this example, we're checking for both.
duckdb.sql(
    f"""
    SELECT DISTINCT
        COUNT(device.ip) AS total_device_ips
        COUNT(file.name) AS total_file_names
    FROM read_json('{LOCAL_JSON}')
    """
).show()

# %%
# To stack-rank unique values, specify a field name and also use the COUNT() operatator. GROUP BY allows you to aggregate, and ORDER BY "field_name/aliAS" with either DESC (descending) or ASC (AScending) allows you to sort by a total. You can place a LIMIT if you only care about the "Top X" amount of unique values.
duckdb.sql(
    f"""
    SELECT
        COUNT(device.ip) AS total_device_ips,
        device.ip AS device_ip
    FROM read_json('{LOCAL_JSON}')
    GROUP BY device_ip
    ORDER BY total_device_ips DESC
    LIMIT 15
    """
).show()

# %%
# To filter rows by a specific value, use the WHERE, also know AS a predicate to find a specific value. It's important to request specific fields to keep the data set smaller if there is complex and/or numerous columns
duckdb.sql(
    f"""
    SELECT DISTINCT
        *
    FROM read_json('{LOCAL_JSON}')
    WHERE device.ip = '188.166.30.169'
    """
).show()

# %%
# You can combine multiple predicates together using AND or OR boolean operators to find very specific details. In this example, imagine needing to find impacted device details, ownership, and malware data from aggregated data in an EDR tool.
duckdb.sql(
    f"""
    SELECT DISTINCT
        device.hostname AS hostname,
        device.owner.name AS username,
        file.sha256_hASh_data.value AS sha256,
        file.path AS file_path,
        file.name AS filename
    FROM read_json('{LOCAL_JSON}')
    WHERE severity = 'Fatal' 
    OR severity = 'Critical'
    AND status = 'In Progress'
    """
).show()

# %%
# Remember, you can use COUNT(*) to audit totals of a specific scenario
duckdb.sql(
    f"""
    SELECT DISTINCT
        COUNT(*) AS total_matching_findings
    FROM read_json('{LOCAL_JSON}')
    WHERE severity = 'Fatal' 
    OR severity = 'Critical'
    AND status = 'In Progress'
    """
).show()

# %%
# Alternatively, AS shown before, use aggregatons instead of predicates to organize the data bASed on severity levels. The "id" is an integer to better order, you do not need to SELECT a specific field to use it in an aggregation or AS a sort key
duckdb.sql(
    f"""
    SELECT DISTINCT
        severity,
        status,
        COUNT(*) AS alert_count
    FROM read_json('{LOCAL_JSON}')
    GROUP BY severity, severity_id, status
    ORDER BY alert_count DESC
    """
).show()

# %%
# Using the HAVING command enables more specific filtering of aggregated results alongside predicates. Combine this with the NOT keyword to create a more specialized filtering when there are multiple values in your source data and you want everything *except* a specific value.
duckdb.sql(
    f"""
    SELECT
        device.hostname AS host,
        device.type AS device_type,
        COUNT(*) AS alert_count
    FROM read_json('{LOCAL_JSON}')
    WHERE status != 'Suppressed'
    GROUP BY device.hostname, device.type
    HAVING alert_count > 3
    """
).show()

# %%
# The DATE_TRUNC() function allows timestamps in a dataset to be truncated to specific internals for more specific intervals in a larger data set. Can be used for quickly ASsessing seASonality or outliers in a dataset
duckdb.sql(
    f"""
    SELECT
        COUNT(*) AS alert_count,
        DATE_TRUNC('hour', time) AS event_hour
    FROM read_json('{LOCAL_JSON}')
    WHERE status != 'Suppressed'
    GROUP BY event_hour
    ORDER BY alert_count DESC
    """
).show()

# %%
# Another way to perform time-bASed analysis is using the EXTRACT() function to extract specific components from a timestamp such AS hours, days, months, etc. This will place the extracted value within the field instead of the truncated timestamp itself which may aid in readability.
duckdb.sql(
    f"""
    SELECT
        EXTRACT(day FROM time) AS event_day,
        EXTRACT(month FROM time) AS event_month,
        EXTRACT(year FROM time) AS event_year,
        COUNT(*) AS alert_count
    FROM read_json('{LOCAL_JSON}')
    WHERE status != 'Suppressed'
    GROUP BY event_day, event_month, event_year
    ORDER BY event_day DESC
    """
).show()

# %%
# For pattern matching in fields use the LIKE operator in your predicates. The LIKE command can be used for fuzzy matching in unstructured text fields such AS an event or finding description, title, message, or perhaps a normalized command line or script content from an EDR or Application Performance Monitoring (APM) tool. In this example, you can pull specific fields needed from a report bASed on the malware being found in an SSH path. For more advanced pattern matching consider using regular expressions (regex). DuckDB uses the RE2 library AS its regex engine and supports a wide variety of regex functions AS seen here: https://duckdb.org/docs/sql/functions/regular_expressions.html
duckdb.sql(
    f"""
    SELECT
        device.hostname AS hostname,
        device.ip AS device_ip,
        device.agent.uid AS agent_id,
        file.name AS filename
    FROM read_json('{LOCAL_JSON}')
    WHERE file.path LIKE '%ssh%'
    ORDER BY hostname ASC
    """
).show()

# %%
# The LIKE operator can also be negated using NOT, additionally, case insensitive matching can utilize the ILIKE operators instead
duckdb.sql(
    f"""
    SELECT
        device.hostname AS hostname,
        device.ip AS device_ip,
        device.agent.uid AS agent_id,
        file.name AS filename
    FROM read_json('{LOCAL_JSON}')
    WHERE file.path NOT LIKE '%C%'
    AND finding_info.attack_technique.name ILIKE '%shadow%'
    ORDER BY hostname ASC
    """
).show()

# %%
# To provide limited data transformation and conditional logic in queries use the CASE statement. This can be helpful for determining your own severities or impact scoring, or for any other case where conditionally creating a custom field in a dataset is useful for an analysis. This can be combined with other statements and functions for specialized use cases, for instance, in the following example you're picking some fields and creating an impact bASed on OS versions. https://duckdb.org/docs/sql/expressions/case
duckdb.sql(
    f"""
    SELECT DISTINCT
        device.hostname AS hostname,
        device.ip AS device_ip,
        CASE
            WHEN device.os.name = 'CentOS' AND device.os.version = '8' THEN 'High Impact'
            WHEN device.os.name = 'CentOS' AND device.os.version = '8.2' THEN 'Medium Impact'
            WHEN device.os.name = 'CentOS' AND device.os.version = '9' THEN 'Low Impact'
            ELSE 'No Impact'
        END AS impact_level
    FROM read_json('{LOCAL_JSON}')
    WHERE device.os.name = 'CentOS'
    GROUP BY device.hostname, device.ip, device.os.name, device.os.version
    ORDER BY hostname ASC
    """
).show()

# %%
# When working with data that is incomplete, or the completeness is unknown consider using the COALESCE function which returns the first non-null value of the field. On its own returning the first non-null value may not be useful but consider a dataset such AS this one which contains the `device.owner` object with `email_addr`, `name` and `uid` fields. Some or all of these could be empty depending on the data completeness. COALESCE can be used to insert missing data to avoid nulls in aggregations, handle missing timestamps, or combining different fields such AS those in an external system such AS Microsoft Defender XDR which can provide datasets that have `ProcessId`, `OriginatingProcessId`, and `OriginatingProcessParentId` which some, or all of them blank.
duckdb.sql(
    f"""
    SELECT DISTINCT
        device.hostname AS hostname,
        COALESCE(device.owner.uid, device.owner.email_addr, device.owner.domain, 'Unknown') AS device_owner
    FROM read_json('{LOCAL_JSON}')
    ORDER BY hostname ASC
    """
).show()

# %%
# Basic mathematical function can be used in datasets for calculating averages, maximum and minimum thresholds, and even summing values. A good candidate for using these are the normalized `id` values within the OCSF such AS `severity_id` or `risk_level_id` to quickly find statistical variations in larger OCSF dataset. This would prove more useful for network-bASed logs that log traffic payloads such AS WAF or load balancer logs.
duckdb.sql(
    f"""
    SELECT
        AVG(risk_level_id) AS avg_risk,
        AVG(severity_id) AS avg_sev,
        MAX(risk_level_id) AS max_risk,
        MAX(severity_id) AS max_sev,
        MIN(risk_level_id) AS min_risk,
        MIN(severity_id) AS min_sev
    FROM read_json('{LOCAL_JSON}')
    """
).show()

# %%
# For basic trend analysis or anomaly detection - or at least, anomaly observation - consider using window functions. The ROW_NUMBER() function ASsigns row numbers for ranking data within a partition of data. Using the PARTITION BY function you can create partitions of data and further order them by the time in the dataset, this can be useful in network-bASed activity logs or across larger historical EDR data. This isn't the best dataset to use this for AS it's limit to 1000 rows.
duckdb.sql(
    f"""
    SELECT DISTINCT
        device.hostname AS hostname,
        finding_info.title as finding_title,
        finding_info.attack_technique.uid as attack_technique_id,
        file.name as filename,
        ROW_NUMBER() OVER (
            PARTITION BY 
                file.name
                ORDER BY time
        ) AS malware_count
    FROM read_json('{LOCAL_JSON}')
    ORDER BY malware_count DESC
    """
).show()