"""Windowed features checked against a hand-computed example.

Timeline for card c1 (offsets in seconds from base):
    e1  t=0    $10  M1
    e2  t=40   $20  M2
    e3  t=90   $30  M1   <- scored event, history = [e1, e2]

Windows for e3 use (t - W, t]:
    1m  (30, 90]   -> e2, e3        count 2
    10m (-510, 90] -> e1, e2, e3    count 3, sum 60
    1h             -> e1, e2, e3    sum 60, merchants {M1, M2} = 2
    velocity_10m   -> 60 / 10 min = 6.0 $/min
    seconds since previous -> 90 - 40 = 50
"""

from datetime import datetime, timedelta, timezone

from streaming_fraud.events import TransactionEvent
from streaming_fraud.features import (
    compute_card_features,
    compute_features_for_stream,
    window_count,
)

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def ev(event_id, card, offset_s, amount, merchant):
    return TransactionEvent(
        event_id=event_id,
        card_id=card,
        timestamp=BASE + timedelta(seconds=offset_s),
        amount=amount,
        merchant=merchant,
    )


def test_hand_computed_features():
    e1 = ev("a", "c1", 0, 10.0, "M1")
    e2 = ev("b", "c1", 40, 20.0, "M2")
    e3 = ev("c", "c1", 90, 30.0, "M1")

    f = compute_card_features([e1, e2], e3)

    assert f["amount"] == 30.0
    assert f["txn_count_1m"] == 2
    assert f["txn_count_10m"] == 3
    assert f["txn_count_1h"] == 3
    assert f["amount_sum_10m"] == 60.0
    assert f["amount_sum_1h"] == 60.0
    assert f["amount_velocity_10m"] == 6.0
    assert f["distinct_merchants_1h"] == 2
    assert f["seconds_since_prev_txn"] == 50.0


def test_window_boundary_is_half_open():
    # An event at exactly t - W is outside (t - W, t].
    at_edge = ev("x", "c1", 30, 5.0, "M1")
    at_t = ev("y", "c1", 90, 5.0, "M1")
    assert window_count([at_edge, at_t], at_t.timestamp, 60) == 1


def test_first_event_for_card():
    e1 = ev("a", "c1", 0, 10.0, "M1")
    f = compute_card_features([], e1)
    assert f["txn_count_1h"] == 1
    assert f["seconds_since_prev_txn"] == -1.0
    assert f["distinct_merchants_1h"] == 1


def test_future_events_never_leak_into_features():
    e1 = ev("a", "c1", 0, 10.0, "M1")
    e2 = ev("b", "c1", 40, 20.0, "M2")
    future = ev("z", "c1", 5000, 999.0, "M9")
    f = compute_card_features([e1, e2, future], e2)
    assert f["txn_count_1h"] == 2
    assert f["amount_sum_1h"] == 30.0


def test_other_cards_do_not_contaminate():
    e1 = ev("a", "c1", 0, 10.0, "M1")
    other = ev("b", "c2", 10, 20.0, "M2")
    e3 = ev("c", "c1", 90, 30.0, "M1")
    f = compute_card_features([e1, other], e3)
    assert f["txn_count_1h"] == 2


def test_stream_features_match_pointwise():
    events = [ev("a", "c1", 0, 10.0, "M1"), ev("b", "c1", 40, 20.0, "M2"),
              ev("c", "c1", 90, 30.0, "M1")]
    stream = compute_features_for_stream(events)
    assert stream[2] == compute_card_features(events[:2], events[2])
    assert stream[0]["txn_count_1h"] == 1
