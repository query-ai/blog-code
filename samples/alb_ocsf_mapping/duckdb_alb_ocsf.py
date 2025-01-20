# %%
import duckdb

LOCAL_PARQUET = "awsalb_ocsf_http_activity.parquet.zstd"

# %%
duckdb.sql(
    f"""
    SELECT COUNT(*) FROM read_parquet('{LOCAL_PARQUET}')
    """
).show()

# %%
duckdb.sql(
    f"""
    SELECT * FROM read_parquet('{LOCAL_PARQUET}')
    LIMIT 30
    """
).show()

# %%
duckdb.sql(
    f"""
    SELECT DISTINCT
        src_endpoint.ip,
        dst_endpoint.uid
    FROM read_parquet('{LOCAL_PARQUET}')
    LIMIT 50
    """
).show()

# %%
duckdb.sql(
    f"""
    SELECT
        COUNT(activity_name) AS total_methods,
        status_code,
        activity_name as http_method
    FROM read_parquet('{LOCAL_PARQUET}')
    GROUP BY http_method, status_code
    ORDER BY total_methods DESC
    """
).show()

# %%
duckdb.sql(
    f"""
    SELECT
        src_endpoint.ip as src_ip,
        src_endpoint.port as src_port,
        dst_endpoint.ip as dst_ip,
        dst_endpoint.port as dst_port,
        http_request.user_agent as user_agent,
        message,
        status_detail
    FROM read_parquet('{LOCAL_PARQUET}')
    WHERE status_code = 460
    """
).show()

# %%
duckdb.sql(
    f"""
    SELECT
        activity_name as http_method,
        status,
        src_endpoint.ip,
        http_request.url.query_string as query_string
    FROM read_parquet('{LOCAL_PARQUET}')
    WHERE http_request.url.query_string IS NOT NULL
    """
).show()