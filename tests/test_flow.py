import pytest

pytest.importorskip("bytewax", reason="streaming dataflow tests need bytewax")

from streaming_fraud.events import generate_events  # noqa: E402
from streaming_fraud.flow import run_flow  # noqa: E402
from streaming_fraud.scorer import FraudScorer  # noqa: E402
from streaming_fraud.store import FeatureStore  # noqa: E402


def test_flow_scores_every_event_with_rules_scorer():
    events = generate_events(n=120, seed=5)
    records = run_flow(events, FraudScorer())  # no model -> rules path
    assert len(records) == len(events)
    assert all(r["decision"]["source"] == "rules" for r in records)

    fraud_records = [
        r for r in records if r["event"]["is_fraud_label"]
    ]
    assert fraud_records, "expected labelled fraud in the generated stream"
    # The last event of each burst has full window context and must block.
    assert any(r["decision"]["decision"] == "block" for r in fraud_records)


def test_flow_writes_feature_snapshots():
    events = generate_events(n=30, seed=6)
    with FeatureStore() as store:
        run_flow(events, FraudScorer(), store=store)
        assert store.count_snapshots() == len(events)
