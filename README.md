# Streaming Fraud Capstone — Real-Time Inference

**Status:** Scaffold built — runnable · **Roadmap stage:** Advanced (Project P6, capstone)
Part of the [ML / AI Engineer Roadmap](https://github.com/NanaAkwasiAbayieBoateng/ai-ml-engineer-roadmap) — full plan in the roadmap's [P6 deep-dive](https://github.com/NanaAkwasiAbayieBoateng/ai-ml-engineer-roadmap/blob/main/projects/p6-streaming-capstone.md).

📖 New here? Read [BLOG_POST.md](BLOG_POST.md) — a file-by-file walkthrough of the whole project.

The [fraud scorer](https://github.com/NanaAkwasiAbayieBoateng/end_to_end_fraud_ml_project) grown into a real-time system: streaming transaction events → windowed features → a feature store with point-in-time-correct reads → a scoring service (demo model + rules fallback). Everything runs **locally and free**: no cloud account, no paid services, and the default path needs no Kafka cluster.

## Run commands

Copy-paste, in order, from the repo root:

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies (free / open-source only)
pip install -r requirements.txt

# 3. Train the demo model (synthetic data, saved to models/demo_model.joblib)
python scripts/train_demo_model.py

# 4. Run the end-to-end local demo — no Kafka needed
python scripts/run_local_demo.py

# 5. Produce synthetic events to a JSONL file (or stdout without -o)
python -m streaming_fraud.producer --n 200 -o events.jsonl

# 6. Run the test suite
python -m pytest -q

# 7. Full streaming mode with Redpanda (Kafka-compatible broker)
docker compose up -d redpanda
docker compose run --rm producer   # publishes 200 events to the transactions topic
docker compose up app              # scoring service
docker compose down                # stop everything
```

Optional, only for publishing to a broker from the host:
`pip install -r requirements-kafka.txt`, then
`python -m streaming_fraud.producer --kafka --n 200` (broker at `localhost:19092`).

## How it works

- **`streaming_fraud/events.py`** — transaction event schema (dataclass + JSON) and a deterministic synthetic generator with labelled fraud bursts.
- **`streaming_fraud/features.py`** — pure windowed-feature functions (per-card counts and amount sums over trailing 1m/10m/1h windows, amount velocity, distinct merchants). One implementation shared by streaming, batch, and training — no train/serve skew.
- **`streaming_fraud/store.py`** — SQLite feature store. `get_features_asof(card_id, t)` returns the most recent snapshot computed at or before `t` — never features from later events. That point-in-time guarantee is the point of a feature store, and it is unit-tested.
- **`streaming_fraud/scorer.py`** — demo sklearn model when available; deterministic threshold rules decide whenever the model is missing or fails. Every decision records which path produced it.
- **`streaming_fraud/flow.py`** — Bytewax dataflow wiring it together (keyed by card → stateful features → scorer → output). Bytewax is imported defensively: the core modules and tests work without it.
- **`scripts/run_local_demo.py`** — the whole pipeline in one process, with a locally measured events/sec rate and a point-in-time sanity check.
- **`docs/tradeoffs.md`** — the architecture write-up: streaming vs batch, SQLite vs Redis, the rules-fallback rationale, cost per 1k events, and the 10× plan (measured numbers included, blanks marked where a load test is still owed).
- **`infra/terraform/`** — a commented single-VM skeleton for the day this outgrows a laptop. Not applied; no credentials committed.

## Plan (from the roadmap)

1. **Event source:** transaction events to Kafka (or Kinesis on AWS), schema defined up front. ✅ schema + producer; Redpanda locally
2. **Stream processing:** Bytewax computing windowed features — transactions per card per hour, amount velocity, merchant aggregates. ✅
3. **Feature store:** online features with point-in-time joins so training and serving cannot disagree. ✅ SQLite snapshots (see trade-offs for the Redis migration trigger)
4. **Scoring service:** the registered model scores each event using online features; every decision logged with its feature snapshot. ✅ demo model + decision records
5. **Cloud deployment:** Terraform single-VM skeleton. 🟡 skeleton only, not applied
6. **Load test:** sustained events/sec and p95 latency. 🟡 local rate measured (~1,650 events/sec, see trade-offs); broker-mode load test still owed
7. **Safety + monitoring:** drift/data-quality alerts into the P5 dashboards; fallback to simple rules if the model service degrades. ✅ rules fallback; drift hooks are P5's job
8. **Write-up:** architecture + trade-offs document. ✅ `docs/tradeoffs.md`

**Elective:** a LoRA/QLoRA fine-tune of a small open model for a narrow task, evaluated against the prompting baseline with the P5 harness.

## Done when

Live events are scored end-to-end with measured throughput and latency, and the trade-off document exists. (Local path: done. Broker-mode load numbers: still owed — marked `[TODO]` in the trade-offs doc rather than invented.)

## Stack

Python · Bytewax · Redpanda (Kafka-compatible) · SQLite feature store · scikit-learn · Terraform skeleton · Docker
