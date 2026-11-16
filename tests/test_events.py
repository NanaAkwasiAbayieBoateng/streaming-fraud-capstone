from datetime import timezone

from streaming_fraud.events import (
    TransactionEvent,
    events_from_jsonl,
    events_to_jsonl,
    generate_events,
)


def test_event_json_round_trip():
    event = generate_events(n=5, seed=1)[0]
    restored = TransactionEvent.from_json(event.to_json())
    assert restored == event
    assert restored.timestamp.tzinfo is not None
    assert restored.timestamp.utcoffset() == timezone.utc.utcoffset(None)


def test_jsonl_round_trip():
    events = generate_events(n=25, seed=2)
    restored = events_from_jsonl(events_to_jsonl(events))
    assert restored == events


def test_generator_is_deterministic():
    a = generate_events(n=50, seed=99)
    b = generate_events(n=50, seed=99)
    assert a == b
    c = generate_events(n=50, seed=100)
    assert a != c


def test_generator_output_is_sorted_and_labelled():
    events = generate_events(n=200, seed=42)
    timestamps = [e.timestamp for e in events]
    assert timestamps == sorted(timestamps)
    fraud = [e for e in events if e.is_fraud_label]
    assert fraud, "expected some labelled fraud bursts"
    # Fraud bursts: same card, large amounts, tight timing.
    assert all(e.amount >= 600 for e in fraud)
    assert all(e.merchant == "CRYPTO_EXCHANGE_Z" for e in fraud)
