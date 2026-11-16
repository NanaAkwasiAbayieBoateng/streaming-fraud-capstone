import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

from streaming_fraud.events import TransactionEvent, generate_events
from streaming_fraud.features import compute_card_features, compute_features_for_stream
from streaming_fraud.scorer import BLOCK, APPROVE, FraudScorer, rules_decision

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def ev(event_id, offset_s, amount, merchant="SHOP"):
    return TransactionEvent(
        event_id=event_id,
        card_id="c1",
        timestamp=BASE + timedelta(seconds=offset_s),
        amount=amount,
        merchant=merchant,
    )


def burst():
    """Three large transactions inside 90 seconds — the classic burst."""
    history = [ev("a", 0, 700.0, "CRYPTO_EXCHANGE_Z"), ev("b", 45, 800.0, "CRYPTO_EXCHANGE_Z")]
    current = ev("c", 90, 900.0, "CRYPTO_EXCHANGE_Z")
    return history, current


def test_rules_block_burst_without_any_model():
    history, current = burst()
    features = compute_card_features(history, current)
    decision = rules_decision(current, features)
    assert decision.decision == BLOCK
    assert decision.source == "rules"
    assert decision.score is None


def test_rules_approve_ordinary_purchase():
    current = ev("a", 0, 24.50, "COFFEE_HOUSE")
    features = compute_card_features([], current)
    assert rules_decision(current, features).decision == APPROVE


def test_scorer_falls_back_when_model_file_missing(tmp_path):
    scorer = FraudScorer(model_path=tmp_path / "does_not_exist.joblib")
    assert scorer.model_available is False
    assert scorer.model_error  # reason recorded, not raised
    history, current = burst()
    decision = scorer.score(current, compute_card_features(history, current))
    assert decision.source == "rules"
    assert decision.decision == BLOCK


def test_scorer_falls_back_when_model_unset():
    scorer = FraudScorer()
    assert scorer.model_available is False
    current = ev("a", 0, 12.0)
    decision = scorer.score(current, compute_card_features([], current))
    assert decision.source == "rules"
    assert decision.decision == APPROVE


def _load_train_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "train_demo_model.py"
    spec = importlib.util.spec_from_file_location("train_demo_model", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_demo_model_end_to_end_on_synthetic_events(tmp_path):
    """Train the demo model with the real training script, then score a
    fresh synthetic stream with it: bursts flagged, ordinary spend approved."""
    import joblib

    train_module = _load_train_module()
    model, metrics = train_module.train_demo_model(n=2000, seed=3)
    assert metrics["holdout_roc_auc"] > 0.8

    model_path = tmp_path / "demo_model.joblib"
    joblib.dump(model, model_path)
    scorer = FraudScorer(model_path=model_path)
    assert scorer.model_available is True

    events = generate_events(n=300, seed=77)
    features = compute_features_for_stream(events)
    decisions = [scorer.score(e, f) for e, f in zip(events, features)]
    assert all(d.source == "model" for d in decisions)

    fraud_flags = [
        d.decision in ("block", "review")
        for e, d in zip(events, decisions)
        if e.is_fraud_label
    ]
    assert fraud_flags and all(fraud_flags), "model should flag the fraud bursts"

    legit_approvals = [
        d.decision == "approve"
        for e, d in zip(events, decisions)
        if e.is_fraud_label is False
    ]
    assert sum(legit_approvals) / len(legit_approvals) > 0.9
