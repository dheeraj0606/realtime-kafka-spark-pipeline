"""
test_aggregation.py

Unit test for the windowed aggregation logic used in glue_streaming_job.py.
Runs against static sample data (no Kinesis, no AWS needed) so the
transformation logic can be verified quickly and cheaply.

Usage:
    pip install pyspark pytest
    pytest test_aggregation.py
"""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("aggregation-unit-test")
        .master("local[*]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_windowed_count_by_category(spark):
    sample_events = [
        ("e1", "user_1", "view", "P100", "electronics", 799.99, datetime(2026, 9, 28, 10, 0, 5)),
        ("e2", "user_2", "view", "P101", "electronics", 149.50, datetime(2026, 9, 28, 10, 0, 20)),
        ("e3", "user_3", "purchase", "P102", "home", 39.99, datetime(2026, 9, 28, 10, 0, 40)),
        ("e4", "user_1", "view", "P100", "electronics", 799.99, datetime(2026, 9, 28, 10, 1, 10)),
    ]
    columns = ["event_id", "user_id", "event_type", "product_id", "category", "price", "event_timestamp"]
    df = spark.createDataFrame(sample_events, columns)

    result = (
        df.groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("category"),
        )
        .agg(count("*").alias("event_count"))
        .select(col("window.start").alias("window_start"), "category", "event_count")
        .orderBy("window_start", "category")
        .collect()
    )

    # First minute: 2 electronics events + 1 home event
    # Second minute: 1 electronics event
    assert len(result) == 3

    first_minute_electronics = [r for r in result if r["category"] == "electronics" and r["event_count"] == 2]
    assert len(first_minute_electronics) == 1

    home_events = [r for r in result if r["category"] == "home"]
    assert home_events[0]["event_count"] == 1
