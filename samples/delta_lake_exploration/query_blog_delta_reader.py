# %%
import duckdb

S3_BUCKET_NAME = ""
S3_DELTA_PATH = f"s3://{S3_BUCKET_NAME}/deltalake/source=query_blog_lakehouse"

# %%
duckdb.sql(f"""
    CREATE SECRET delta_lake (
        TYPE S3,
        PROVIDER credential_chain
    )
"""
)
print("Created S3 Secret!")

# %%
duckdb.sql(f"""
    SELECT 
        year,
        month,
        day,
        COUNT(*) AS event_count
    FROM delta_scan('{S3_DELTA_PATH}')
    GROUP BY year, month, day
    ORDER BY year DESC, month DESC, day DESC
"""
).show()

# %%
duckdb.sql(f"""
    SELECT DISTINCT
        client_ip
    FROM delta_scan('{S3_DELTA_PATH}')
    WHERE direction = 'in' 
        AND action NOT IN ('block', 'drop')
        AND internal_port NOT IN (80, 443)
"""
).show()

# %%
duckdb.sql(f"""
    SELECT
        client_ip,
        COUNT(*) AS event_count
    FROM delta_scan('{S3_DELTA_PATH}')
    WHERE direction = 'in' 
    AND action NOT IN ('block', 'drop')
    AND internal_port NOT IN (80, 443)
    GROUP BY client_ip
    ORDER BY event_count DESC
    LIMIT 20
"""
).show()

# %%
duckdb.sql(f"""
    SELECT 
        event_time,
        client_ip,
        internal_ip,
        internal_port,
        client_port,
        ROW_NUMBER() OVER (PARTITION BY internal_ip ORDER BY event_time DESC) AS row_num
    FROM delta_scan('{S3_DELTA_PATH}')
    WHERE action = 'block' AND internal_port = 443
"""
).show()

# %%
duckdb.sql(f"""
    SELECT 
        client_ip, 
        SUM(total_bytes) AS total_transfer
    FROM delta_scan('{S3_DELTA_PATH}')
    WHERE action NOT IN ('block','drop')
        AND direction = 'outbound'
    GROUP BY client_ip
    ORDER BY total_transfer DESC
    LIMIT 10
"""
).show()