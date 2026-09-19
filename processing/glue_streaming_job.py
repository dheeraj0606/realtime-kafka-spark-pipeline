"""
glue_streaming_job.py

AWS Glue Structured Streaming job: reads from the Kinesis stream,
parses the JSON payload, and (next session) computes a windowed
aggregation and writes Parquet to S3.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col

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

# TODO next session: windowed aggregation (count by category per 1-min window),
# write the aggregated result to S3 as Parquet, and refresh the Glue Catalog table.


def process_batch(data_frame, batch_id):
    """Placeholder per-microbatch handler -- just proves the read+parse works."""
    print(f"Batch {batch_id}: {data_frame.count()} events parsed")
    data_frame.show(5, truncate=False)


glueContext.forEachBatch(
    frame=parsed_stream,
    batch_function=process_batch,
    options={
        "windowSize": "30 seconds",
        "checkpointLocation": args["s3_output_path"] + "/checkpoint/",
    },
)

job.commit()
