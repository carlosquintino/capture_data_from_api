CREATE TABLE IF NOT EXISTS logs.capture_api (
    start_exec TIMESTAMP,
    job_name STRING,
    status STRING,
    table_name STRING,
    source STRING,
    info STRING,
    error STRING,
    error_desc STRING,
    date_ref STRING,
    end_exec  TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/case-datalake/logs/capture_api/';
