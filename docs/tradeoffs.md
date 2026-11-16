# Architecture trade-offs — Streaming Fraud Capstone

Decisions are filled in; measured numbers are marked **[MEASURED]** with
the date and setup, and **[TODO]** where a load test still has to run.
A trade-off doc with invented numbers is worse than one with blanks.

## System in one paragraph

Transaction events → windowed features (pure functions, one
implementation shared by streaming, batch, and training) → feature
snapshots in a store with point-in-time-correct reads → scorer (small
ML model, deterministic rules fallback) → decision records. Everything
runs locally with free/open-source components; the cloud story is one
VM (see `infra/terraform/`), not a platform.

## Decision 1: Streaming vs batch scoring

**Chosen: streaming (event-at-a-time), with the batch path kept for
training and backfills.**

- The features that catch fraud bursts — count/amount velocity over
  1–60 minute windows — decay in minutes. A batch job scoring hourly
  sees the burst after the money is gone.
- Event-at-a-time scoring keeps the decision path identical online and
  in tests: one event in, one decision out, no micro-batch semantics
  to debug.
- Cost of the choice: ordering and state management move into the
  stream processor (Bytewax keyed state here). Accepted, because the
  state is small (per-card history inside a 1-hour window) and the
  pure feature functions make it testable without the processor.
- Batch is still the right tool for training data generation and
  backfills; `compute_features_for_stream` over a sorted file *is*
  the batch path — same code, no skew.

## Decision 2: Feature store — SQLite vs Redis

**Chosen: SQLite snapshots for this project; Redis when multi-process
reads or sub-millisecond lookups become real requirements.**

- The non-negotiable property is **point-in-time correctness**:
  `get_features_asof(card, t)` must never return features computed
  from events after `t`. SQLite gives that with one indexed query
  (`computed_at <= t ORDER BY computed_at DESC`) plus transactions
  and durability for free.
- Redis would win on concurrent-writer throughput and lookup latency,
  but point-in-time reads need sorted-set gymnastics per card, and
  durability is configuration-dependent. At demo scale (thousands of
  events/sec, single writer) SQLite is not the bottleneck — the
  Python feature computation is.
- Migration trigger: sustained writes above ~5k/sec from multiple
  processes, or a second service needing the same features online.
  The store's interface (write snapshot / as-of read) is deliberately
  the two operations a Redis implementation would also expose.

## Decision 3: Rules fallback when the model is unavailable

**Chosen: the scorer never fails open. Missing/corrupt model or a
scoring exception → deterministic threshold rules decide, and every
decision records its `source`.**

- A fraud endpoint that 500s (or worse, approves everything) during a
  model-deploy incident is an outage with financial blast radius.
  Rules are worse than the model on average but bounded, explainable,
  and always available.
- The fallback rate (`source == "rules"` share) is itself an alerting
  metric: if it is ever nonzero in production, something is broken.
- Rules thresholds live in one module (`scorer.py`) so the fallback
  behavior is code-reviewed like any other model change.

## Decision 4: Bytewax vs Kafka Streams / Flink

**Chosen: Bytewax.** Python-native (the feature/scoring code imports
directly — no JVM bridge, no separate feature implementation),
single-process locally, scales to multiple workers with the same
dataflow. Kafka Streams would tie the project to the JVM; Flink is
operationally heavy for a one-VM deployment. The dataflow is a thin
shell around pure functions, so swapping processors later is a
wiring change, not a rewrite.

## Decision 5: Redpanda vs Apache Kafka (local streaming mode)

**Chosen: Redpanda** for the optional broker mode — Kafka-API
compatible, single binary/container, free (Apache 2.0 core), no JVM
or ZooKeeper/KRaft tuning for a laptop. Anything that speaks the
Kafka protocol (including the producer here) works unchanged against
either.

## Cost model — per 1,000 events

| Setup | Marginal cost per 1k events | Notes |
|---|---|---|
| Local (this repo, dev machine) | **$0** | Electricity only; measured throughput below |
| Single free-tier VM, 24/7 | **~$0** within free tier; ≈ $0.00004/1k at $10/mo and a sustained 100 events/sec | One t3.micro-class VM ≈ 259M events/mo at 100 ev/s |
| What breaks first at 10× | — | See below |

**At 10× volume** (~15–20k events/sec against this design):
1. Python feature computation saturates CPU first → Bytewax scales
   out by keying on card across workers/processes; the pure functions
   do not change.
2. SQLite write contention → feature store moves to Redis (Decision
   2's migration trigger), snapshots become a write-through cache.
3. Broker partitions: the transactions topic needs partitioning by
   card_id (already the producer key) so ordering per card survives
   scale-out.
4. Cost driver becomes broker + store infrastructure, not scoring;
   budget roughly one small instance per ~5k events/sec sustained.

## Measured numbers

- **[MEASURED 2026-11-15]** Local demo pipeline (features + SQLite
  snapshot writes + model scoring, single process, shared 1.5 vCPU
  VM, in-memory store): **1,982 events in 1.20 s ≈ 1,650 events/sec**.
  Reproduce: `python scripts/run_local_demo.py --n 2000`.
- **[MEASURED 2026-11-15]** Demo model quality on synthetic data:
  holdout ROC-AUC **1.000**. Caveat: the synthetic fraud pattern is
  separable by construction — this validates the plumbing, not a
  real-world detection rate.
- **[TODO]** Bytewax dataflow throughput (events/sec, 1 worker) vs
  the pure-Python path above.
- **[TODO]** Sustained load test against Redpanda mode: max stable
  events/sec and p95 end-to-end latency (produce → decision).
- **[TODO]** p95 scoring latency per event under load (feature
  computation + model inference only).

## Known limitations (honest list)

- Synthetic data only; no real fraud distribution, no concept drift
  handling yet (that is the P5 eval harness's job in the roadmap).
- Feature state is in-process memory in the Bytewax mapper; a worker
  restart replays from the source — fine for a finite demo source,
  needs checkpointing/recovery configuration for a real deployment.
- The demo model is a teaching artifact (LogisticRegression on nine
  features). The capstone's claim is the *system*, not the model.
