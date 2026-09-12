# Core concepts

- Kafka topic: named event stream, split into partitions for parallelism
- Partition: ordered log; order guaranteed only within a partition
- Consumer group: consumers share partitions between them
- Spark Structured Streaming: treats the stream as a growing table, re-runs the query on new data every micro-batch
