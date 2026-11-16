"""SQLite feature store with point-in-time-correct lookups.

Why SQLite here: it is free, embedded, transactional, and more than fast
enough for a single-node streaming demo — see docs/tradeoffs.md for when
Redis would replace it.

The store keeps one *snapshot* per scored event: the feature dict
computed for that event, keyed by the event's timestamp. The lookup
contract is the whole point of a feature store:

    get_features_asof(card_id, t) returns the most recent snapshot with
    computed_at <= t — never a snapshot produced by a later event.

That is what prevents label/feature leakage when offline training data
is joined against online events: training rows get exactly the features
that were knowable at scoring time.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .events import TransactionEvent


def _ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


class FeatureStore:
    """Append-only snapshot store backed by a SQLite file (or :memory:)."""

    def __init__(self, path: str | Path = ":memory:"):
        self._conn = sqlite3.connect(str(path))
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                card_id     TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                event_id    TEXT,
                features    TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_card_time "
            "ON feature_snapshots (card_id, computed_at)"
        )
        self._conn.commit()

    def write_features(
        self,
        card_id: str,
        timestamp: datetime,
        features: dict[str, float],
        event_id: str | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO feature_snapshots (card_id, computed_at, event_id, features) "
            "VALUES (?, ?, ?, ?)",
            (card_id, _ts(timestamp), event_id, json.dumps(features, sort_keys=True)),
        )
        self._conn.commit()

    def write_event_features(
        self, event: TransactionEvent, features: dict[str, float]
    ) -> None:
        self.write_features(event.card_id, event.timestamp, features, event.event_id)

    def get_features_asof(
        self, card_id: str, timestamp: datetime
    ) -> dict[str, float] | None:
        """Most recent snapshot at or before ``timestamp``; None if none exists.

        Snapshots computed from later events are structurally invisible
        to this query — that is the point-in-time guarantee.
        """
        row = self._conn.execute(
            "SELECT features FROM feature_snapshots "
            "WHERE card_id = ? AND computed_at <= ? "
            "ORDER BY computed_at DESC LIMIT 1",
            (card_id, _ts(timestamp)),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def count_snapshots(self, card_id: str | None = None) -> int:
        if card_id is None:
            row = self._conn.execute("SELECT COUNT(*) FROM feature_snapshots").fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM feature_snapshots WHERE card_id = ?", (card_id,)
            ).fetchone()
        return int(row[0])

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "FeatureStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
