"""Transaction event schema and a deterministic synthetic event generator.

An event is one card transaction. The schema is deliberately small and
stable: every downstream component (features, store, scorer) consumes
exactly this shape, whether events arrive from the synthetic generator,
a JSONL file, or a Kafka/Redpanda topic.
"""

from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class TransactionEvent:
    """One card transaction event.

    ``timestamp`` is timezone-aware UTC. Amounts are in minor units of the
    currency-agnostic demo currency (think cents) to avoid float money.
    """

    event_id: str
    card_id: str
    timestamp: datetime
    amount: float
    merchant: str
    category: str = "retail"
    channel: str = "online"  # online | pos | atm
    is_fraud_label: bool | None = field(default=None, compare=False)

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "card_id": self.card_id,
            "timestamp": self.timestamp.astimezone(timezone.utc).isoformat(),
            "amount": self.amount,
            "merchant": self.merchant,
            "category": self.category,
            "channel": self.channel,
            "is_fraud_label": self.is_fraud_label,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict) -> "TransactionEvent":
        ts = data["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return cls(
            event_id=str(data["event_id"]),
            card_id=str(data["card_id"]),
            timestamp=ts,
            amount=float(data["amount"]),
            merchant=str(data["merchant"]),
            category=str(data.get("category", "retail")),
            channel=str(data.get("channel", "online")),
            is_fraud_label=data.get("is_fraud_label"),
        )

    @classmethod
    def from_json(cls, line: str) -> "TransactionEvent":
        return cls.from_dict(json.loads(line))


_MERCHANTS = [
    ("AMAZON", "retail"),
    ("GROCERY_MART", "grocery"),
    ("COFFEE_HOUSE", "food"),
    ("AIRLINE_X", "travel"),
    ("GAS_STATION_9", "fuel"),
    ("STREAMING_CO", "subscriptions"),
    ("PHARMACY_PLUS", "health"),
    ("ELECTRONICS_HUB", "retail"),
]

_CARDS = [f"card_{i:03d}" for i in range(1, 21)]


def generate_events(
    n: int = 200,
    seed: int = 42,
    start: datetime | None = None,
    fraud_rate: float = 0.04,
) -> list[TransactionEvent]:
    """Generate ``n`` synthetic events deterministically.

    Most events are ordinary small purchases spread over a few hours.
    A small share are labelled fraud bursts: several large transactions
    on one card in a short window at an unusual merchant — exactly the
    pattern the windowed features are designed to catch.

    The returned list is sorted by timestamp.
    """
    rng = random.Random(seed)
    start = start or datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    events: list[TransactionEvent] = []
    cursor = start

    fraud_bursts = max(1, int(n * fraud_rate / 3)) if n >= 10 else 0
    burst_cards = rng.sample(_CARDS, min(fraud_bursts, len(_CARDS))) if fraud_bursts else []

    normal_target = n - fraud_bursts * 3
    for i in range(normal_target):
        cursor += timedelta(seconds=rng.randint(5, 120))
        merchant, category = rng.choice(_MERCHANTS)
        events.append(
            TransactionEvent(
                event_id=f"evt_{len(events):06d}",
                card_id=rng.choice(_CARDS),
                timestamp=cursor,
                amount=round(rng.uniform(3.0, 180.0), 2),
                merchant=merchant,
                category=category,
                channel=rng.choice(["online", "pos", "online"]),
                is_fraud_label=False,
            )
        )

    # Fraud bursts are spread across the full time span of the normal
    # stream, so any chronological slice (e.g. a training holdout)
    # contains both classes.
    span_seconds = max((cursor - start).total_seconds(), 600.0)
    for card in burst_cards:
        offset = rng.uniform(0.05 * span_seconds, 0.95 * span_seconds)
        burst_start = start + timedelta(seconds=offset)
        for j in range(3):
            events.append(
                TransactionEvent(
                    event_id=f"evt_{len(events):06d}",
                    card_id=card,
                    timestamp=burst_start + timedelta(seconds=45 * j),
                    amount=round(rng.uniform(600.0, 950.0), 2),
                    merchant="CRYPTO_EXCHANGE_Z",
                    category="crypto",
                    channel="online",
                    is_fraud_label=True,
                )
            )

    events.sort(key=lambda e: e.timestamp)
    # Re-number ids after sorting so ids stay monotonic in time.
    events = [
        TransactionEvent(
            event_id=f"evt_{i:06d}",
            card_id=e.card_id,
            timestamp=e.timestamp,
            amount=e.amount,
            merchant=e.merchant,
            category=e.category,
            channel=e.channel,
            is_fraud_label=e.is_fraud_label,
        )
        for i, e in enumerate(events)
    ]
    return events


def events_to_jsonl(events: list[TransactionEvent]) -> str:
    return "\n".join(e.to_json() for e in events) + ("\n" if events else "")


def events_from_jsonl(text: str) -> list[TransactionEvent]:
    return [TransactionEvent.from_json(line) for line in text.splitlines() if line.strip()]


__all__ = [
    "TransactionEvent",
    "generate_events",
    "events_to_jsonl",
    "events_from_jsonl",
    "asdict",
]
