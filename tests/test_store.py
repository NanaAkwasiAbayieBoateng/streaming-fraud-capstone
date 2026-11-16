"""Point-in-time correctness: a lookup as of time t must return the
snapshot that was knowable at t, even after later snapshots are written.
"""

from datetime import datetime, timedelta, timezone

from streaming_fraud.store import FeatureStore

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def at(seconds):
    return BASE + timedelta(seconds=seconds)


def test_asof_returns_latest_snapshot_at_or_before():
    with FeatureStore() as store:
        store.write_features("c1", at(100), {"v": 1.0}, event_id="e1")
        store.write_features("c1", at(300), {"v": 3.0}, event_id="e3")

        assert store.get_features_asof("c1", at(99)) is None
        assert store.get_features_asof("c1", at(100)) == {"v": 1.0}
        assert store.get_features_asof("c1", at(299)) == {"v": 1.0}
        assert store.get_features_asof("c1", at(300)) == {"v": 3.0}
        assert store.get_features_asof("c1", at(10_000)) == {"v": 3.0}


def test_later_writes_never_change_earlier_lookups():
    with FeatureStore() as store:
        store.write_features("c1", at(100), {"txn_count_1h": 1.0}, event_id="e1")
        before = store.get_features_asof("c1", at(150))

        # Snapshots computed from events AFTER the as-of time must be
        # invisible to the lookup — that is the point-in-time guarantee.
        for i in range(2, 8):
            store.write_features(
                "c1", at(200 + i * 10), {"txn_count_1h": float(i)}, event_id=f"e{i}"
            )

        after = store.get_features_asof("c1", at(150))
        assert before == after == {"txn_count_1h": 1.0}


def test_cards_are_isolated():
    with FeatureStore() as store:
        store.write_features("c1", at(100), {"v": 1.0})
        assert store.get_features_asof("c2", at(100)) is None
        assert store.count_snapshots("c1") == 1
        assert store.count_snapshots() == 1


def test_file_backed_store_persists(tmp_path):
    path = tmp_path / "features.db"
    with FeatureStore(path) as store:
        store.write_features("c1", at(5), {"v": 9.0})
    with FeatureStore(path) as reopened:
        assert reopened.get_features_asof("c1", at(5)) == {"v": 9.0}
