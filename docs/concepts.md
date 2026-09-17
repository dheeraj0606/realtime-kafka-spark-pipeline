## Pipeline stages (Kinesis -> S3)

1. Kinesis stream buffers events durably (24h retention)
2. Glue Structured Streaming job reads it as a live table, triggering every 30-60s
3. Windowed aggregation runs on each micro-batch
4. Results written to S3 as Parquet, partitioned by date/hour
5. Glue Catalog table keeps schema in sync with the S3 files
6. Athena queries the table like a live dashboard

Note: this is near-real-time via micro-batching, not true per-event
continuous processing.
