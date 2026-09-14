"""
producer.py

Generates synthetic e-commerce clickstream events and publishes them
continuously to an AWS Kinesis Data Stream.

Usage:
    export KINESIS_STREAM_NAME=user-events
    export AWS_REGION=us-east-1
    export EVENTS_PER_SECOND=2
    python producer.py

Requires AWS credentials to already be configured (via `aws configure`,
environment variables, or an IAM role) — boto3 picks these up automatically.
"""

import os
import json
import time
import random
import uuid
import logging
from datetime import datetime, timezone

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("producer")

STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "user-events")
REGION = os.environ.get("AWS_REGION", "us-east-1")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "2"))

PRODUCTS = [
    {"product_id": "P100", "category": "electronics", "price": 799.99},
    {"product_id": "P101", "category": "electronics", "price": 149.50},
    {"product_id": "P102", "category": "home", "price": 39.99},
    {"product_id": "P103", "category": "books", "price": 14.99},
    {"product_id": "P104", "category": "clothing", "price": 59.00},
]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.7, 0.2, 0.1]  # most events are views, fewer are purchases


def generate_event() -> dict:
    """Build one synthetic clickstream event."""
    product = random.choice(PRODUCTS)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 500)}",
        "event_type": event_type,
        "product_id": product["product_id"],
        "category": product["category"],
        "price": product["price"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    client = boto3.client("kinesis", region_name=REGION)
    interval = 1.0 / EVENTS_PER_SECOND

    logger.info(
        "Starting producer -> stream=%s region=%s rate=%.2f events/sec",
        STREAM_NAME, REGION, EVENTS_PER_SECOND,
    )

    sent = 0
    try:
        while True:
            event = generate_event()
            client.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(event).encode("utf-8"),
                PartitionKey=event["user_id"],
            )
            sent += 1
            if sent % 20 == 0:
                logger.info("Sent %d events so far", sent)

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Stopped by user. Total events sent: %d", sent)
    except Exception:
        logger.exception("Producer crashed after sending %d events", sent)
        raise


if __name__ == "__main__":
    main()
