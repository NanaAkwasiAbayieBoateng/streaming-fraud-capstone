"""Scoring: demo ML model first, deterministic rules as the fallback.

A production scoring path must never fail open because a model artifact
is missing or a model call throws. The contract here:

- If a model is loaded and scores cleanly, its probability decides.
- Otherwise (no model file, load failure, scoring exception) the rules
  decide. Rules are boring on purpose: a few thresholds an on-call human
  can reason about at 3 a.m.

Every decision records which path produced it (``source``), so the
fallback rate is itself a monitorable metric.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .events import TransactionEvent
from .features import feature_vector

APPROVE = "approve"
REVIEW = "review"
BLOCK = "block"


@dataclass(frozen=True)
class Decision:
    decision: str  # approve | review | block
    score: float | None  # model probability, or None on the rules path
    source: str  # "model" | "rules"
    reason: str

    def to_dict(self) -> dict:
        return {
            "decision": self.decision,
            "score": self.score,
            "source": self.source,
            "reason": self.reason,
        }


# --- Rules fallback -------------------------------------------------------
# Thresholds are module constants so tests and docs can reference them.

RULE_BLOCK_AMOUNT = 500.0
RULE_BLOCK_COUNT_10M = 3
RULE_BLOCK_VELOCITY_10M = 150.0  # dollars per minute over trailing 10 min
RULE_REVIEW_AMOUNT = 300.0
RULE_REVIEW_COUNT_1M = 3


def rules_decision(event: TransactionEvent, features: dict[str, float]) -> Decision:
    """Deterministic threshold rules. No model involved."""
    if (
        event.amount >= RULE_BLOCK_AMOUNT
        and features.get("txn_count_10m", 0) >= RULE_BLOCK_COUNT_10M
    ):
        return Decision(
            BLOCK,
            None,
            "rules",
            f"burst: amount {event.amount:.2f} with "
            f"{int(features['txn_count_10m'])} txns in 10m",
        )
    if features.get("amount_velocity_10m", 0) >= RULE_BLOCK_VELOCITY_10M:
        return Decision(
            BLOCK,
            None,
            "rules",
            f"velocity: {features['amount_velocity_10m']:.1f} $/min over 10m",
        )
    if event.amount >= RULE_REVIEW_AMOUNT:
        return Decision(REVIEW, None, "rules", f"high amount {event.amount:.2f}")
    if features.get("txn_count_1m", 0) >= RULE_REVIEW_COUNT_1M:
        return Decision(
            REVIEW,
            None,
            "rules",
            f"rapid fire: {int(features['txn_count_1m'])} txns in 1m",
        )
    return Decision(APPROVE, None, "rules", "no rule triggered")


class FraudScorer:
    """Scores events with an optional sklearn model; rules on any failure."""

    def __init__(
        self,
        model_path: str | Path | None = None,
        block_threshold: float = 0.8,
        review_threshold: float = 0.4,
    ):
        self.block_threshold = block_threshold
        self.review_threshold = review_threshold
        self.model = None
        self.model_error: str | None = None
        if model_path is not None:
            self._load(model_path)

    def _load(self, model_path: str | Path) -> None:
        path = Path(model_path)
        if not path.is_file():
            self.model_error = f"model file not found: {path}"
            return
        try:
            import joblib

            self.model = joblib.load(path)
        except Exception as exc:  # corrupt artifact etc. -> rules take over
            self.model_error = f"model load failed: {exc}"
            self.model = None

    @property
    def model_available(self) -> bool:
        return self.model is not None

    def score(self, event: TransactionEvent, features: dict[str, float]) -> Decision:
        if self.model is not None:
            try:
                proba = float(self.model.predict_proba([feature_vector(features)])[0][1])
            except Exception as exc:
                fallback = rules_decision(event, features)
                return Decision(
                    fallback.decision,
                    None,
                    "rules",
                    f"model error ({exc}); rules fallback: {fallback.reason}",
                )
            if proba >= self.block_threshold:
                return Decision(BLOCK, proba, "model", f"model p(fraud)={proba:.3f} >= {self.block_threshold}")
            if proba >= self.review_threshold:
                return Decision(REVIEW, proba, "model", f"model p(fraud)={proba:.3f} >= {self.review_threshold}")
            return Decision(APPROVE, proba, "model", f"model p(fraud)={proba:.3f}")
        return rules_decision(event, features)
