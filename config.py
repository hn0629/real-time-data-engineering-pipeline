# config.py

KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
KAFKA_TOPIC = "test-topic-clean"

CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "realtime_pipeline"
CASSANDRA_EVENTS_TABLE = "events"
CASSANDRA_METRICS_TABLE = "source_metrics"

RAW_CHECKPOINT_PATH = "/tmp/checkpoints/raw_events"
RAW_INVALID_CHECKPOINT_PATH = "/tmp/checkpoints/raw_events_invalid"

METRICS_CHECKPOINT_PATH = "/tmp/checkpoints/source_metrics"
METRICS_INVALID_CHECKPOINT_PATH = "/tmp/checkpoints/source_metrics_invalid"

ALLOWED_EVENT_TYPES = ["view", "add_to_cart", "purchase"]

RAW_LOG_FILE = "spark_kafka_to_cassandra.log"
METRICS_LOG_FILE = "spark_kafka_source_metrics.log"
