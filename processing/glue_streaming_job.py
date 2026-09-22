"""
glue_streaming_job.py

AWS Glue Structured Streaming job: reads from the Kinesis stream,
parses the JSON payload, aggregates event counts per category in
1-minute windows, and writes the results to S3 as Parquet.

Next session: register/refresh a Glue Catalog table over the S3
output (via a Crawler, or glueContext catalog write options) so the
data is queryable in Athena.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, window, count

# --- Job setup ---
args = getResolvedOptions(sys.argv, ["JOB_NAME", "kinesis_stream_arn", "s3_output_path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Schema for our synthetic clickstream events (matches producer.py) ---
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("event_timestamp", TimestampType()),
])

# --- Read the raw stream from Kinesis ---
raw_stream = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "streamARN": args["kinesis_stream_arn"],
        "startingPosition": "LATEST",
        "inferSchema": "false",
        "classification": "json",
    },
    transformation_ctx="raw_stream",
)

# --- Parse the JSON payload into structured columns ---
parsed_stream = raw_stream.select(
    from_json(col("data").cast("string"), event_schema).alias("event")
).select("event.*")

# --- Windowed aggregation: event count per category, per 1-minute window ---
# The watermark tells Spark how long to wait for late-arriving events
# (up to 2 minutes late) before it finalizes and emits a window.
aggregated_stream = (
    parsed_stream
    .withWatermark("event_timestamp", "2 minutes")
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("category"),
    )
    .agg(count("*").alias("event_count"))
)

# Flatten the window struct into plain columns so the output is easier
# to query later (Athena doesn't love nested struct columns as much).
output_stream = aggregated_stream.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("category"),
    col("event_count"),
)

# --- Write the aggregated results to S3 as Parquet ---
query = (
    output_stream.writeStream
    .format("parquet")
    .option("path", args["s3_output_path"] + "/data/")
    .option("checkpointLocation", args["s3_output_path"] + "/checkpoint/")
    .trigger(processingTime="30 seconds")
    .outputMode("append")
    .start()
)

# This job runs continuously until manually stopped in the Glue console --
# job.commit() is not reached while the stream is active.
query.awaitTermination()
