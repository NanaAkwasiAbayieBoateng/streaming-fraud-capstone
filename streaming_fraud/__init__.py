"""Streaming Fraud Capstone — real-time fraud inference, locally and free.

Pipeline: transaction events -> windowed features -> SQLite feature store
(point-in-time correct) -> scorer (demo model with rules fallback).

The core logic (events, features, store, scorer) is pure Python / pandas /
scikit-learn and is fully testable without Kafka, Redpanda, or Bytewax
running. Bytewax is only needed for the streaming dataflow in
:mod:`streaming_fraud.flow`.
"""

__all__ = ["events", "features", "store", "scorer"]
