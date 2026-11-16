"""Train the tiny demo model on synthetic data and save it with joblib.

The demo model is a LogisticRegression over the windowed features. It
exists so the scorer's model path (and its failure modes) can be
exercised end-to-end with zero external data — it is a teaching model,
not a production fraud model.

Usage:
    python scripts/train_demo_model.py [--output models/demo_model.joblib]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import joblib  # noqa: E402
from sklearn.linear_model import LogisticRegression  # noqa: E402
from sklearn.metrics import roc_auc_score  # noqa: E402
from sklearn.pipeline import Pipeline  # noqa: E402
from sklearn.preprocessing import StandardScaler  # noqa: E402

from streaming_fraud.events import generate_events  # noqa: E402
from streaming_fraud.features import compute_features_for_stream, feature_vector  # noqa: E402


def train_demo_model(n: int = 4000, seed: int = 7):
    """Train on the first 70% (chronologically), evaluate on the last 30%."""
    events = generate_events(n=n, seed=seed)
    features = compute_features_for_stream(events)
    X = [feature_vector(f) for f in features]
    y = [1 if e.is_fraud_label else 0 for e in events]

    split = int(len(X) * 0.7)
    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=1000)),
        ]
    )
    model.fit(X[:split], y[:split])
    proba = model.predict_proba(X[split:])[:, 1]
    auc = roc_auc_score(y[split:], proba)
    return model, {"holdout_roc_auc": float(auc), "train_rows": split, "test_rows": len(X) - split}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="models/demo_model.joblib")
    parser.add_argument("--n", type=int, default=4000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    model, metrics = train_demo_model(n=args.n, seed=args.seed)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, out)
    print(
        f"saved demo model to {out} "
        f"(holdout ROC-AUC {metrics['holdout_roc_auc']:.3f}, "
        f"train {metrics['train_rows']}, test {metrics['test_rows']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
