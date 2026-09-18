"""
glue_streaming_job.py

AWS Glue Structured Streaming job: reads from the Kinesis stream,
computes a windowed aggregation, and writes Parquet to S3.

TODO:
- Read stream from Kinesis (source: "kinesis")
- Parse JSON event payload into a structured schema
- Apply watermark + windowed aggregation (count by category per 1-min window)
- Write stream to S3 as Parquet, partitioned by date/hour
- Register/refresh Glue Catalog table
"""

# Glue job boilerplate and imports go here next session
def main():
    pass


if __name__ == "__main__":
    main()
