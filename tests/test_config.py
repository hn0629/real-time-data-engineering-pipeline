import config


def test_allowed_event_types():
    assert config.ALLOWED_EVENT_TYPES == [
        "view",
        "add_to_cart",
        "purchase",
    ]


def test_cassandra_events_table_name():
    assert config.CASSANDRA_EVENTS_TABLE == "events"


def test_cassandra_metrics_table_name():
    assert config.CASSANDRA_METRICS_TABLE == "source_metrics"


def test_kafka_topic_name():
    assert config.KAFKA_TOPIC == "test-topic-clean"


def test_cassandra_keyspace_name():
    assert config.CASSANDRA_KEYSPACE == "realtime_pipeline"