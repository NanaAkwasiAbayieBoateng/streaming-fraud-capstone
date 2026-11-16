"""End-to-end local demo — no Kafka, no Redpanda, no cloud.

Generates synthetic events, computes windowed features, writes feature
snapshots to an in-memory feature store, scores every event (demo model
if present, rules fallback otherwise), and prints a summary including
the locally measured processing rate.

Usage:
    python scripts/run_local_demo.py [--n 2000] [--seed 123]
    python scripts/run_local_demo.py --rules-only
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streaming_fraud.events import generate_events  # noqa: E402
from streaming_fraud.features import compute_card_features  # noqa: E402
from streaming_fraud.scorer import FraudScorer  # noqa: E402
from streaming_fraud.store import FeatureStore  # noqa: E402

MODEL_PATH = Path(__file__).resolve().parents[1] / "models" / "demo_model.joblib"


def ensure_model() -> Path | None:
    """Train the demo model on first run so the demo works out of the box."""
    if MODEL_PATH.is_file():
        return MODEL_PATH
    scripts_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(scripts_dir))
    import train_demo_model

    model, metrics = train_demo_model.train_demo_model()
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    import joblib

    joblib.dump(model, MODEL_PATH)
    print(
        f"trained demo model -> {MODEL_PATH} "
        f"(holdout ROC-AUC {metrics['holdout_roc_auc']:.3f})"
    )
    return MODEL_PATH


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument(
        "--rules-only",
        action="store_true",
        help="skip the model and score with the rules fallback only",
    )
    args = parser.parse_args()

    events = generate_events(n=args.n, seed=args.seed)
    scorer = FraudScorer() if args.rules_only else FraudScorer(model_path=ensure_model())
    mode = "rules" if args.rules_only or not scorer.model_available else "model"
    if not args.rules_only and not scorer.model_available:
        print(f"note: model unavailable ({scorer.model_error}); rules fallback active")

    store = FeatureStore(":memory:")
    by_card: dict[str, list] = {}
    decisions = []
    labelled = Counter()
    labelled_correct = Counter()

    start = time.perf_counter()
    for event in events:
        history = by_card.setdefault(event.card_id, [])
        features = compute_card_features(history, event)
        store.write_event_features(event, features)
        decision = scorer.score(event, features)
        history.append(event)
        decisions.append((event, decision))
        if event.is_fraud_label is not None:
            key = "fraud" if event.is_fraud_label else "legit"
            labelled[key] += 1
            if (decision.decision in ("block", "review")) == bool(event.is_fraud_label):
                labelled_correct[key] += 1
    elapsed = time.perf_counter() - start
    rate = len(events) / elapsed if elapsed > 0 else float("inf")

    counts = Counter(d.decision for _, d in decisions)
    sources = Counter(d.source for _, d in decisions)
    print(f"\n=== streaming fraud demo ({mode} scoring) ===")
    print(f"events processed : {len(events)} in {elapsed:.2f}s  ({rate:,.0f} events/sec)")
    print(f"feature snapshots: {store.count_snapshots()} in SQLite store")
    print(f"decisions        : {dict(counts)}")
    print(f"decision sources : {dict(sources)}")
    if labelled["fraud"]:
        print(
            f"fraud bursts     : {labelled_correct['fraud']}/{labelled['fraud']} "
            "flagged (block/review)"
        )
    if labelled["legit"]:
        print(
            f"legit events     : {labelled_correct['legit']}/{labelled['legit']} "
            "approved"
        )

    flagged = [(e, d) for e, d in decisions if d.decision != "approve"][:5]
    if flagged:
        print("\nsample flagged decisions:")
        for event, decision in flagged:
            print(
                f"  {event.timestamp.isoformat()} {event.card_id} "
                f"${event.amount:>7.2f} @ {event.merchant:<18} "
                f"-> {decision.decision.upper():<6} [{decision.source}] {decision.reason}"
            )

    # Point-in-time lookup sanity: features as of the first event must be
    # that event's own snapshot, unaffected by everything written later.
    first = events[0]
    asof = store.get_features_asof(first.card_id, first.timestamp)
    print(
        f"\npoint-in-time check: features as of first event "
        f"({first.card_id} @ {first.timestamp.isoformat()}) -> "
        f"txn_count_1h={asof['txn_count_1h'] if asof else None} (expected 1.0)"
    )
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
