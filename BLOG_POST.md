# Streaming Fraud Capstone — a file-by-file walkthrough

*How this project was built, why each tool is here, and what every file in the repo does.*

## 1. What this project does

This project scores card transactions **as they happen**, one event at a time:

```
transaction event → windowed features → feature store → scorer → decision
```

For each transaction it asks questions like *"how many transactions has this card made in the last 10 minutes, and how much money moved?"* — the classic shape of a fraud burst is several large purchases on one card in a few minutes. Those windowed features are stored in a small feature store, fed to a model (with simple rules as a safety net), and out comes a decision: **approve**, **review**, or **block**.

Everything runs **locally and for free**: no cloud account, no paid services, and the default demo path needs no message broker at all.

### How this differs from the P1+P2 fraud scorer

The earlier project in this roadmap ([end_to_end_fraud_ml_project](https://github.com/NanaAkwasiAbayieBoateng/end_to_end_fraud_ml_project)) is a **batch / request-response** scorer: a FastAPI service that scores one transaction per HTTP request, using features sent with the request, plus batch scoring and drift checks on files. This capstone changes the *system shape*, not just the model:

| | P1+P2 fraud scorer | This capstone (P6) |
|---|---|---|
| Trigger | HTTP request / batch file | A stream of events |
| Features | Sent in with the request | Computed from the card's recent history, over time windows |
| Feature state | None — stateless per request | A feature store with point-in-time-correct reads |
| New problems solved | Model gating, registry, drift | Ordering, window boundaries, training/serving skew, never failing open |
| Model | XGBoost classifier behind a quality gate | Tiny demo model — deliberately boring; the *system* is the project |

The lesson this project teaches: in real-time fraud, the hard part is rarely the model. It is computing the same features online and offline, never letting future data leak into a decision, and still making *some* defensible decision when the model is down.

## 2. The tools, and why each one

**Bytewax** (`streaming_fraud/flow.py`) — the stream processor. The mainstream streaming stacks (Kafka Streams, Flink) live on the JVM, which would mean a second language and a second implementation of the feature logic. Bytewax is Python-native: the dataflow imports the exact same feature and scoring functions used in tests and training. It also runs in a single process on a laptop and scales out by key (card) later. The dataflow is a thin shell around pure functions, so swapping processors one day is a wiring change, not a rewrite.

**Redpanda** (`docker-compose.yml`) — the optional broker for "full streaming mode." It speaks the Kafka API but ships as a single container with no JVM and no ZooKeeper/KRaft tuning — the lightest way to get a real broker on a laptop. Anything that talks the Kafka protocol works against Redpanda or Apache Kafka unchanged. It is *optional*: the default demo path reads events from the generator or a JSONL file.

**SQLite** (`streaming_fraud/store.py`) — the feature store. A feature store's essential job here is one query: *"give me the most recent features for this card as of time t."* SQLite answers that with one indexed SQL query, is transactional and durable, is embedded (no server to run), and is free. `docs/tradeoffs.md` documents the exit ramp: move to **Redis** when there are sustained writes above ~5k/sec from multiple processes, or a second service needs the same features online. At this project's scale, SQLite is not the bottleneck — the Python feature computation is.

**scikit-learn** (`scripts/train_demo_model.py`) — the demo model is a `LogisticRegression` over the nine windowed features, saved with **joblib**. It exists so the scorer's model path — loading, scoring, failing — can be exercised end to end with zero external data. It is a teaching model, not a production fraud model, and the docs say so.

**kafka-python, kept optional** (`requirements-kafka.txt`) — the producer's `--kafka` mode is the only code path that needs a Kafka client, so the dependency lives in its own requirements file. The JSONL path — the default — pulls in no broker client at all.

**Terraform skeleton** (`infra/terraform/main.tf`) — one small VM running the whole stack under Docker Compose, for the day the project outgrows a laptop. It is a *starting point only*: **not applied**, no credentials committed, and the file itself tells you to run `terraform plan` and review before any `apply`.

One honest note: `requirements.txt` also installs **pandas** (part of the standard data stack), but no code in the repo currently imports it — the pipeline is pure standard library plus scikit-learn. It is there for future analysis work, not because the pipeline needs it.

## 3. How it was built, step by step

The build order was deliberate: pure, testable core first; streaming wiring last.

1. **Event schema + synthetic generator** (`events.py`). Nothing can be built until "a transaction" is defined. The generator came with it, so every later stage had realistic data — including labelled fraud bursts to aim the features at.
2. **Pure feature functions** (`features.py`). Windowed features as plain functions over sorted event lists: no framework, no I/O. This is the heart of the system, so it was built where it could be checked against hand-computed examples.
3. **Feature store** (`store.py`). Snapshot writes and the point-in-time read, with the "later writes never change earlier lookups" property tested directly.
4. **Scorer with rules fallback** (`scorer.py`). Model first, deterministic rules whenever the model is missing or fails — designed before any model existed, because the fallback contract shapes the `Decision` type everything else consumes.
5. **Demo model training** (`scripts/train_demo_model.py`). Only now does a model appear: trained on features produced by step 2's code, evaluated on a chronological holdout.
6. **Bytewax wiring** (`flow.py`). The streaming shell: source → key by card → stateful features + scoring → sink. Swapping the test source for a Kafka source later is a one-line change by design.
7. **Producer CLI** (`producer.py`). JSONL to stdout/file by default; Kafka mode behind a flag and an optional dependency.
8. **Local demo, compose, Terraform** (`scripts/run_local_demo.py`, `docker-compose.yml`, `infra/terraform/main.tf`). The demo proves the whole pipeline in one process and measures it; compose adds the broker; Terraform sketches the one-VM cloud story.

## 4. Every file, explained

Read the repo in this order and it will make sense top to bottom.

### The package: `streaming_fraud/`

#### `streaming_fraud/__init__.py`
The package docstring: a five-line summary of the pipeline and the key design promise — the core (events, features, store, scorer) is testable **without** Kafka, Redpanda, or Bytewax running. It exports the four core module names.

#### `streaming_fraud/events.py` — what a transaction is
Defines `TransactionEvent`, a frozen dataclass: `event_id`, `card_id`, timezone-aware UTC `timestamp`, `amount`, `merchant`, plus `category`, `channel`, and an optional `is_fraud_label` used only by the synthetic data and evaluation. `to_json()` / `from_json()` (and the `events_to_jsonl` / `events_from_jsonl` helpers) give every component one shared wire format — the same JSON shape whether an event comes from the generator, a file, or a Kafka topic.

`generate_events(n, seed, ...)` is the deterministic synthetic generator. Most events are small ordinary purchases (20 cards, 8 merchants, a few seconds to two minutes apart). A small share are **fraud bursts**: 3 transactions of $600–$950, 45 seconds apart, at `CRYPTO_EXCHANGE_Z` — exactly the pattern the features are built to catch. Same seed → same stream, every time, which is what makes the tests and demo numbers reproducible. Events come back sorted by timestamp, with IDs re-numbered so they stay monotonic in time.

#### `streaming_fraud/features.py` — the heart of the system
Pure functions, no side effects, no framework. This is the **only** implementation of the features — the streaming flow, the batch demo, and model training all call it, because train/serve skew usually starts with two implementations of the same feature.

`compute_card_features(card_history, event)` returns nine features (`FEATURE_NAMES` fixes their canonical order for the model): the amount, transaction counts over 1m/10m/1h windows, amount sums over 10m/1h, `amount_velocity_10m` (dollars per minute over the trailing 10 minutes), distinct merchants in the last hour, and seconds since the previous transaction (−1.0 for a card's first event).

The idea that matters most here is the **window convention**: for an event at time *t* and window *W*, the window is the half-open interval **(t − W, t]** — strictly after *t − W*, up to and including *t*. An event landing exactly on the boundary *t − W* is **outside** the window (there is a test for exactly this). The event being scored is included in its own windows — it is part of the burst it may belong to — and events after *t* are never included, even if a too-large history list contains them. `compute_features_for_stream()` applies the same logic to a whole sorted stream, each event seeing only events at or before its own timestamp; `feature_vector()` turns a feature dict into the model's input list in `FEATURE_NAMES` order.

#### `streaming_fraud/store.py` — the feature store
A `FeatureStore` class over one SQLite table, `feature_snapshots`: one row per scored event holding that event's feature dict as JSON, keyed by `(card_id, computed_at)` with an index on the pair. `write_event_features(event, features)` stores a snapshot; `count_snapshots()` and a context manager round out the API. The path can be a file or `:memory:` (the demo and tests use memory).

The whole point of the file is one method:

```sql
SELECT features FROM feature_snapshots
WHERE card_id = ? AND computed_at <= ?
ORDER BY computed_at DESC LIMIT 1
```

`get_features_asof(card_id, t)` returns the most recent snapshot **at or before** *t* — snapshots computed from later events are structurally invisible to the query. That is **point-in-time correctness**, and it is what prevents leakage: when training data is built by joining labels against this store, every row gets exactly the features that were knowable at scoring time, never features that peek at the future. Without it you get training/serving skew — a model that looks great offline because its features cheated, and disappoints online.

#### `streaming_fraud/scorer.py` — model first, rules always
Defines `Decision` (decision, score, source, reason) and `FraudScorer`. The contract: **the scoring path never fails open.**

- If a model artifact was given, it is loaded with joblib. A missing file or a corrupt artifact does not raise — the error is recorded on `scorer.model_error` and the rules take over.
- `score(event, features)` asks the model for `p(fraud)`: ≥ 0.8 → block, ≥ 0.4 → review, else approve. If the model call itself throws, the rules decide and the reason string says so.
- `rules_decision()` is the deterministic fallback: block on a burst (amount ≥ $500 **and** ≥ 3 transactions in 10 minutes) or on velocity (≥ $150/min over 10 minutes); review on a high amount (≥ $300) or rapid fire (≥ 3 transactions in 1 minute); otherwise approve. The thresholds are module-level constants (`RULE_BLOCK_AMOUNT`, etc.) so tests and docs can reference the same numbers.

Every `Decision` carries a **`source`** field — `"model"` or `"rules"`. That makes the fallback rate a monitorable metric: in production, if the share of `source == "rules"` is ever nonzero, something is broken and should page someone.

#### `streaming_fraud/flow.py` — the Bytewax wiring
The streaming shell around the pure core. Bytewax is imported inside a `try` block that sets `_HAS_BYTEWAX`; if Bytewax is absent, the module still imports, and only calling `build_flow()` / `run_flow()` raises a clear error via `require_bytewax()`. Nothing else in the package imports this module, so the core never depends on Bytewax being installed.

The topology is: `TestingSource` → `key_on(card_id)` → `stateful_map` → sink. The stateful mapper keeps each card's event history in Bytewax state, computes features with `compute_card_features` (the same function from `features.py`), optionally writes a snapshot to the feature store, scores, and emits one record per event: `{event, features, decision}`. Two sinks are provided: `CollectSink` (appends records to a list — used by tests) and `PrintSink` (one JSON line per record). `run_flow(events, scorer, store)` runs the whole thing in-process over a finite list and returns the decision records.

Finite runs use Bytewax's `TestingSource` on purpose — it signals end-of-input correctly. A deployed version swaps that one line for the Kafka source connector reading the transactions topic; everything downstream stays identical.

#### `streaming_fraud/producer.py` — the event source CLI
`python -m streaming_fraud.producer` generates events and writes them as JSONL to stdout (default) or a file with `-o`. With `--kafka` it publishes to a Kafka-compatible broker instead (default `localhost:19092`, topic `transactions`, keyed by `card_id` so per-card ordering survives partitioning). The `kafka` import happens only inside that branch; if kafka-python is missing it exits with code 2 and a message telling you to install `requirements-kafka.txt`.

### The scripts: `scripts/`

#### `scripts/train_demo_model.py`
Trains the demo model and saves it to `models/demo_model.joblib`. It generates 4,000 events (seed 7), computes features with `compute_features_for_stream`, and trains a `StandardScaler` → `LogisticRegression` pipeline on the **first 70% chronologically**, reporting ROC-AUC on the last 30%. The chronological split matters: a shuffled split would let the model train on the future of the same cards it is tested on. (On this synthetic data the holdout ROC-AUC comes out at 1.000 — the planted pattern is separable by construction; `docs/tradeoffs.md` is explicit that this validates the plumbing, not a real-world detection rate.)

#### `scripts/run_local_demo.py`
The whole pipeline in one process, no broker: generate events → compute features → write snapshots to an in-memory store → score every event → print a summary. It times the loop and reports events/sec, decision counts, decision sources, how many labelled fraud events were flagged, and how many legit events were approved. `ensure_model()` trains the demo model on first run so the demo works out of the box; `--rules-only` skips the model entirely so you can watch the fallback path decide. It ends with a point-in-time sanity check: the features as of the *first* event must still be that event's own snapshot (`txn_count_1h = 1.0`), unaffected by the ~2,000 snapshots written after it.

### The tests: `tests/` — 21 tests, all passing

Each test file pins down one module's contract:

- **`tests/test_events.py`** (4 tests) — JSON and JSONL round-trips are lossless; the generator is deterministic per seed (and differs across seeds); output is timestamp-sorted and contains labelled bursts with the expected shape (≥ $600, `CRYPTO_EXCHANGE_Z`).
- **`tests/test_features.py`** (6 tests) — the centrepiece is a hand-computed example: three transactions at t = 0/40/90s, with every expected feature value worked out in the docstring (the 1-minute window `(30, 90]` contains 2 events, velocity is 60/10 = 6.0 $/min, and so on). Plus: the half-open boundary (an event at exactly *t − W* is excluded), first-event behaviour, **future events never leak into features**, other cards never contaminate, and stream computation matches pointwise computation.
- **`tests/test_store.py`** (4 tests) — point-in-time correctness stated as tests: a lookup as of *t* returns the latest snapshot ≤ *t*; writing many later snapshots never changes an earlier lookup; cards are isolated; a file-backed store persists across reopen.
- **`tests/test_scorer.py`** (5 tests) — the rules block a hand-built burst and approve a coffee with no model involved; a missing model file falls back to rules *and still blocks the burst*; an unset model falls back too; and an end-to-end test that trains the real demo model via the training script's own function, then scores a fresh stream — every decision tagged `source == "model"`, all fraud bursts flagged, > 90% of legit events approved.
- **`tests/test_flow.py`** (2 tests) — guarded by `pytest.importorskip("bytewax")` so the suite still runs on machines without Bytewax. The flow scores every event (rules path), fraud records include a block, and running with a store writes exactly one snapshot per event.

### Infrastructure and docs

#### `docker-compose.yml`
"Full streaming mode." Three services: **redpanda** (single node, 512M memory, 1 partition by default, health-checked with `rpk cluster health`; Kafka API exposed to host tools on `localhost:19092`, Pandaproxy on 18082, schema registry on 18081), **app** (builds the Dockerfile, mounts `./models`, currently runs the local demo as a skeleton — the comment in the file explains the production wiring swaps the `TestingSource` line in `flow.py` for the Kafka source), and **producer** (behind the `tools` profile, so it only runs when asked: `docker compose run --rm producer`).

#### `Dockerfile`
`python:3.12-slim`, installs both requirements files, copies the package, scripts, and `conftest.py`, and defaults to running the local demo. Small on purpose.

#### `infra/terraform/main.tf`
The one-VM skeleton described in §2: AWS provider, variables for region (default `us-east-1`), instance type (default `t3.micro`, free-tier class), and project name; a data lookup for the latest Ubuntu 22.04 AMI (looked up, never hard-coded); a security group with SSH only (with a TODO comment to tighten the CIDR to your own IP before applying); one instance whose `user_data` installs Docker and would clone the repo and bring the compose stack up; and an output for the instance's public IP. Header comments state plainly: **not applied, no credentials committed, `terraform plan` before any `apply`.**

#### `docs/tradeoffs.md`
The architecture write-up, and the document this blog keeps pointing at. Five decisions, each with the alternative and the reasoning: streaming vs batch; SQLite vs Redis (with the migration trigger); the never-fail-open rules fallback; Bytewax vs Kafka Streams/Flink; Redpanda vs Apache Kafka. Then a cost model per 1,000 events ($0 locally; ~$0 on a free-tier VM), what breaks first at 10× volume, and a measured-numbers section that distinguishes **[MEASURED]** results (with date and setup) from **[TODO]** blanks where a load test is still owed — broker-mode throughput and p95 latency are honestly blank rather than invented. It closes with the known limitations: synthetic data only, in-process feature state in the Bytewax mapper, and a teaching-grade model.

#### `requirements.txt`
The core dependencies, pinned where it matters: `bytewax==0.21.1`, `scikit-learn>=1.5`, `pandas>=2.0` (see the honest note in §2), `joblib>=1.4`, `pytest>=8.0`. Everything free and open source.

#### `requirements-kafka.txt`
One line — `kafka-python>=2.0.2` — plus a comment saying it is only needed for the producer's `--kafka` mode. Keeping it separate means the default install carries no broker client.

#### `conftest.py`
Four lines at the repo root that put the repo root on `sys.path`, so `import streaming_fraud` works when pytest is invoked from anywhere. (This workspace learned that lesson the hard way on an earlier project: bare `pytest` in CI does not add the repo root to the path the way `python -m pytest` does.)

#### `README.md`
The front door: status and roadmap links, the copy-paste **Run commands** (setup → train → demo → producer → tests → compose mode), a "How it works" tour of the main files, the roadmap plan with honest ✅/🟡 markers, and the stack list. If you only read one file, read this one; if you read two, add `docs/tradeoffs.md`.

#### `.gitignore`
Keeps generated artifacts out of git: `__pycache__`, virtualenvs, `.env`, `*.joblib` and `models/*.joblib` (the trained demo model is a build artifact — retrain it with the script), `*.db` (feature-store files), `events.jsonl` (generated event files), `data/`, `mlruns/`, `outputs/`. (There is also a standard MIT `LICENSE` file at the root.)

### Bugs hit during the build (and what fixed them)

Two are worth knowing about, because both are the kind that waste hours:

1. **Bytewax 0.21 API differences.** Several things in 0.21 do not match older examples: the `stateful_map` mapper signature is `(state, value)` — no key argument; keyed streams emit `(key, value)` tuples, so sinks receive tuples and need unwrapping (that is what `_unwrap` in `flow.py` is for); and sink partitions are built per worker via `build(step_id, worker_index, worker_count)`. Worst of all, a hand-rolled source partition never signalled end-of-input and the flow simply **hung** — replaced with Bytewax's `TestingSource`, which handles EOF correctly. All of this is why the flow tests exist.
2. **The generator clustered its fraud bursts too early.** The first version placed all bursts in the opening stretch of the stream. That silently broke chronological evaluation: a train/test split by time could put every fraud event in the training half, leaving a single-class holdout and meaningless metrics. The fix is in `events.py`: bursts are now spread across 5%–95% of the full time span, so any chronological slice contains both classes — and a comment in the code says why.

## 5. A transaction's journey

Follow one real event from the demo run (seed 123): **evt_000167**, the third transaction of a burst on `card_020`.

1. **Producer.** The generator creates it: 2026-01-01 02:51:23 UTC, $818.49 at `CRYPTO_EXCHANGE_Z`, channel online. In file mode it would be one JSON line; in Kafka mode, one message keyed by `card_020`.
2. **History.** The card is not new: it made one ordinary purchase about 43 minutes earlier, then two burst transactions at 02:49:53 ($921.77) and 02:50:38 ($661.08) — 45 seconds apart.
3. **Features.** `compute_card_features` looks back over the half-open windows ending at 02:51:23. The results: `txn_count_1m = 2`, `txn_count_10m = 3`, `txn_count_1h = 4`, `amount_sum_10m = $2,401.34`, `amount_velocity_10m = $240.13/min`, `distinct_merchants_1h = 2`, `seconds_since_prev_txn = 45`. Note the boundary doing quiet work: the ordinary purchase 43 minutes ago counts toward the 1-hour window but not the 10-minute one.
4. **Store.** That feature dict is written as a snapshot keyed by `(card_020, 02:51:23)`. From now on, any as-of query at or after that moment sees these features — and a query as of 02:50:00 still sees the earlier state, untouched.
5. **Scorer.** The demo model reads the nine-feature vector and returns p(fraud) = 1.000 ≥ 0.8 → **BLOCK**, with `source: "model"` and the reason recorded. (Cross-check: had the model been down, the rules would have blocked it too — amount ≥ $500 with 3 transactions in 10 minutes, and velocity $240/min ≥ $150/min.)
6. **Output.** The flow emits one record — event, features, decision — to the sink. In the demo, the summary counts it among the 60 blocked.

## 6. How to run it, what it measured, and what's next

**Run it:** the README's [Run commands](README.md#run-commands) are the canonical list — venv, install, `python scripts/train_demo_model.py`, `python scripts/run_local_demo.py`, `pytest`. The demo needs nothing else running.

**Measured on the build machine** (a shared 1.5 vCPU VM, in-memory store, single process; recorded in `docs/tradeoffs.md` and re-verified when this post was written):

- 1,982 events processed in ~1.2 s — **≈ 1,650 events/sec** for features + snapshot writes + model scoring.
- **60/60** labelled fraud events blocked by the model; **1,922/1,922** legit events approved.
- Demo model holdout ROC-AUC 1.000 on synthetic data (separable by construction — plumbing validation, not a detection claim).
- **21/21 tests pass**, including the suite running without a broker and the flow tests running without Kafka.

**Honest limitations and next steps:**

- **Load testing is still owed.** The `[TODO]` blanks in `docs/tradeoffs.md` — Bytewax dataflow throughput, sustained Redpanda-mode rate, p95 end-to-end latency — are deliberately blank until measured.
- **Synthetic data only.** No real fraud distribution, no concept drift; drift monitoring is the P5 eval harness's job in the roadmap.
- **Redis migration** when the Decision 2 trigger is met (multi-process writes or a second consumer of the features).
- **Feature state recovery.** The Bytewax mapper holds per-card history in process memory; a real deployment needs checkpointing/replay configuration.
- **Cloud deploy.** The Terraform skeleton is ready to review (`terraform plan` first) but intentionally not applied — the project runs locally, on purpose, for learning.
