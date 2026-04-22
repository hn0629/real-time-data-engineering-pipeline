import pandas as pd
import dashboard


class FakeSession:
    def execute(self, query):
        if query == "SELECT * FROM events;":
            return [
                {
                    "event_id": "1",
                    "user_id": "u1",
                    "event_type": "view",
                    "product_id": "p1",
                    "price": "19.99",
                    "event_time": "2026-04-15T18:00:00",
                    "source": "web",
                }
            ]
        if query == "SELECT * FROM source_metrics;":
            return [
                {
                    "source": "web",
                    "event_count": "2",
                    "total_revenue": "39.98",
                }
            ]
        return []


class FakeCluster:
    pass


def test_load_data_converts_columns(monkeypatch):
    def fake_get_cassandra_session():
        return FakeCluster(), FakeSession()

    monkeypatch.setattr(dashboard, "get_cassandra_session", fake_get_cassandra_session)

    events_df, metrics_df = dashboard.load_data()

    assert not events_df.empty
    assert not metrics_df.empty
    assert pd.api.types.is_numeric_dtype(events_df["price"])
    assert pd.api.types.is_datetime64_any_dtype(events_df["event_time"])
    assert pd.api.types.is_numeric_dtype(metrics_df["event_count"])
    assert pd.api.types.is_numeric_dtype(metrics_df["total_revenue"])