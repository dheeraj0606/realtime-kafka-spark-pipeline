-- create_table.sql
--
-- Registers a Glue Catalog table over the Parquet output written by
-- glue_streaming_job.py, so the streaming results are queryable in Athena.
-- Run these statements in the Athena query editor (set a query result
-- location under Settings first, if you haven't already).

CREATE DATABASE IF NOT EXISTS realtime_pipeline_db;

CREATE EXTERNAL TABLE IF NOT EXISTS realtime_pipeline_db.category_event_counts (
    window_start   timestamp,
    window_end     timestamp,
    category       string,
    event_count    bigint
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-NAME/data/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- After the Glue job has been running a few minutes and written some
-- files, verify with:
-- SELECT * FROM realtime_pipeline_db.category_event_counts ORDER BY window_start DESC LIMIT 20;
