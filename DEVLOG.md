## Day X - [today's date]
- Created Kinesis data stream (on-demand mode) for user-events
- Ran producer.py locally, verified IncomingRecords/IncomingBytes ticking up in console
- Created S3 bucket for Parquet sink output
- Created IAM role for Glue with Kinesis read + S3 read/write permissions
- Next: build the Glue Structured Streaming job to consume Kinesis and write to S3
