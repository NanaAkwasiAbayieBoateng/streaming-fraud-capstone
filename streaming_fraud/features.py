"""Windowed feature computation — pure functions over sorted event lists.

All functions here are deterministic and side-effect free so they can be
unit-tested against hand-computed examples and reused identically by the
streaming dataflow, the batch demo, and model training. Train/serve skew
usually starts with two implementations of the same feature; there is
only one implementation, and it is this module.

Window convention: for an event at time ``t`` and a window of ``W``
seconds, the window is the half-open interval ``(t - W, t]`` — events
strictly after ``t - W`` and no later than ``t``. The event being scored
is included in its own windows (it is part of the burst it may belong
to). Events after ``t`` are never included.
"""

from __future__ import annotations

from datetime import timedelta

from .events import TransactionEvent

# Canonical model feature order. Training and serving both use this.
FEATURE_NAMES = [
    "amount",
    "txn_count_1m",
    "txn_count_10m",
    "txn_count_1h",
    "amount_sum_10m",
    "amount_sum_1h",
    "amount_velocity_10m",
    "distinct_merchants_1h",
    "seconds_since_prev_txn",
]

WINDOW_1M = 60
WINDOW_10M = 600
WINDOW_1H = 3600


def _in_window(event_ts, t, window_seconds: int) -> bool:
    return (t - timedelta(seconds=window_seconds)) < event_ts <= t


def window_count(events: list[TransactionEvent], t, window_seconds: int) -> int:
    """Number of events in ``(t - window, t]``."""
    return sum(1 for e in events if _in_window(e.timestamp, t, window_seconds))


def window_amount_sum(events: list[TransactionEvent], t, window_seconds: int) -> float:
    """Total amount of events in ``(t - window, t]``."""
    return sum(e.amount for e in events if _in_window(e.timestamp, t, window_seconds))


def distinct_merchants(events: list[TransactionEvent], t, window_seconds: int) -> int:
    """Distinct merchants seen in ``(t - window, t]``."""
    return len({e.merchant for e in events if _in_window(e.timestamp, t, window_seconds)})


def compute_card_features(
    card_history: list[TransactionEvent],
    event: TransactionEvent,
) -> dict[str, float]:
    """Compute the feature dict for ``event``.

    ``card_history`` is the list of prior events for the same card
    (any order; events after ``event.timestamp`` are ignored, so a
    too-large history is safe). The event itself is added to its own
    windows.
    """
    t = event.timestamp
    past = [e for e in card_history if e.card_id == event.card_id and e.timestamp < t]
    window_events = past + [event]

    amount_sum_10m = window_amount_sum(window_events, t, WINDOW_10M)
    if past:
        seconds_since_prev = (t - max(e.timestamp for e in past)).total_seconds()
    else:
        seconds_since_prev = -1.0

    return {
        "amount": float(event.amount),
        "txn_count_1m": float(window_count(window_events, t, WINDOW_1M)),
        "txn_count_10m": float(window_count(window_events, t, WINDOW_10M)),
        "txn_count_1h": float(window_count(window_events, t, WINDOW_1H)),
        "amount_sum_10m": float(amount_sum_10m),
        "amount_sum_1h": float(window_amount_sum(window_events, t, WINDOW_1H)),
        # Amount velocity: dollars per minute over the trailing 10 minutes.
        "amount_velocity_10m": float(amount_sum_10m) / (WINDOW_10M / 60.0),
        "distinct_merchants_1h": float(distinct_merchants(window_events, t, WINDOW_1H)),
        "seconds_since_prev_txn": float(seconds_since_prev),
    }


def features_for_event(
    all_events: list[TransactionEvent],
    event: TransactionEvent,
) -> dict[str, float]:
    """Convenience wrapper: pick the event's card history out of a full list."""
    history = [e for e in all_events if e.card_id == event.card_id]
    return compute_card_features(history, event)


def compute_features_for_stream(
    events: list[TransactionEvent],
) -> list[dict[str, float]]:
    """Compute features for every event in a timestamp-sorted stream.

    Each event sees only events at or before its own timestamp — the
    same guarantee the feature store's point-in-time lookup enforces.
    """
    by_card: dict[str, list[TransactionEvent]] = {}
    out: list[dict[str, float]] = []
    for event in sorted(events, key=lambda e: e.timestamp):
        history = by_card.setdefault(event.card_id, [])
        out.append(compute_card_features(history, event))
        history.append(event)
    return out


def feature_vector(features: dict[str, float]) -> list[float]:
    """Feature dict -> model input vector, in FEATURE_NAMES order."""
    return [float(features[name]) for name in FEATURE_NAMES]
