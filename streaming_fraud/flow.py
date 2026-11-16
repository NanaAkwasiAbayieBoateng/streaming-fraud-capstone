"""Bytewax dataflow: events -> windowed features -> scorer -> output.

This is the streaming wiring around the pure core. Bytewax is imported
defensively: the rest of the package (events/features/store/scorer) must
import and test cleanly on machines without Bytewax installed, so
nothing else in the package imports this module.

Topology (keyed by card):

    source -> key_on(card_id) -> stateful_map(features + score) -> sink

The stateful mapper keeps each card's event history in Bytewax state,
computes features with the same pure functions used in batch/training,
optionally writes a snapshot to the feature store, and emits one
decision record per event.
"""

from __future__ import annotations

import json
from typing import Iterable

from .events import TransactionEvent
from .features import compute_card_features
from .scorer import FraudScorer
from .store import FeatureStore

try:
    import bytewax.operators as op
    from bytewax.dataflow import Dataflow
    from bytewax.outputs import DynamicSink, StatelessSinkPartition
    from bytewax.testing import TestingSource, run_main

    _HAS_BYTEWAX = True
except Exception:  # pragma: no cover - exercised only without bytewax
    _HAS_BYTEWAX = False


def require_bytewax() -> None:
    if not _HAS_BYTEWAX:
        raise RuntimeError(
            "Bytewax is not installed. Install requirements.txt to use the "
            "streaming dataflow; the core modules work without it."
        )


if _HAS_BYTEWAX:

    def _unwrap(item):
        """Keyed streams emit (key, value) pairs; sinks want the value."""
        if isinstance(item, tuple) and len(item) == 2:
            return item[1]
        return item

    class _CollectPartition(StatelessSinkPartition):
        def __init__(self, out: list):
            self._out = out

        def write_batch(self, items):
            self._out.extend(_unwrap(item) for item in items)

    class CollectSink(DynamicSink):
        """Appends every emitted record to a caller-provided list."""

        def __init__(self, out: list):
            self._out = out

        def build(self, step_id, worker_index, worker_count):
            return _CollectPartition(self._out)

    class _PrintPartition(StatelessSinkPartition):
        def write_batch(self, items):
            for item in items:
                print(json.dumps(_unwrap(item), sort_keys=True), flush=True)

    class PrintSink(DynamicSink):
        """Prints every emitted record as one JSON line."""

        def build(self, step_id, worker_index, worker_count):
            return _PrintPartition()


def build_flow(
    events: Iterable[TransactionEvent],
    scorer: FraudScorer,
    store: FeatureStore | None = None,
    out: list | None = None,
):
    """Build the Dataflow, collecting decision records into ``out``
    (a new list if not provided). Returns (flow, out)."""
    require_bytewax()
    if out is None:
        out = []
    sink = CollectSink(out)

    flow = Dataflow("fraud-scoring")
    # Finite in-process runs use Bytewax's TestingSource, which handles
    # end-of-input correctly. A deployed version would swap this one
    # line for the Kafka source connector reading the transactions topic.
    stream = op.input("events_in", flow, TestingSource(list(events)))
    keyed = op.key_on("key_by_card", stream, lambda e: e.card_id)

    def step(history, event):
        # Bytewax 0.21 stateful_map mapper signature: (state, value).
        # The stream is keyed by card_id, so event.card_id is the key.
        history = history if history is not None else []
        features = compute_card_features(history, event)
        if store is not None:
            store.write_event_features(event, features)
        decision = scorer.score(event, features)
        history = history + [event]
        record = {
            "event": event.to_dict(),
            "features": features,
            "decision": decision.to_dict(),
        }
        return history, record

    scored = op.stateful_map("features_and_score", keyed, step)
    op.output("decisions_out", scored, sink)
    return flow, out


def run_flow(
    events: Iterable[TransactionEvent],
    scorer: FraudScorer,
    store: FeatureStore | None = None,
) -> list[dict]:
    """Run the flow in-process over a finite event list; return decisions."""
    require_bytewax()
    out: list[dict] = []
    flow, _ = build_flow(events, scorer, store=store, out=out)
    run_main(flow)
    return out
