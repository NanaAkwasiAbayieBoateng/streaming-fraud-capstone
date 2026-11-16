# Streaming Fraud Capstone — Real-Time Inference on Cloud

**Status:** Planned · **Roadmap stage:** Advanced (Project P6, capstone)
Part of the [ML / AI Engineer Roadmap](https://github.com/NanaAkwasiAbayieBoateng/ai-ml-engineer-roadmap) — full plan in the roadmap's [P6 deep-dive](https://github.com/NanaAkwasiAbayieBoateng/ai-ml-engineer-roadmap/blob/main/projects/p6-streaming-capstone.md).

The [fraud scorer](https://github.com/NanaAkwasiAbayieBoateng/end_to_end_fraud_ml_project) grown into a real-time system: streaming events, windowed features, a feature store, and cloud-served scoring — reusing the registry and monitoring from the earlier projects.

## Plan

1. **Event source:** transaction events to Kafka (or Kinesis on AWS), schema defined up front.
2. **Stream processing:** Bytewax or Kafka Streams computing windowed features — transactions per card per hour, amount velocity, merchant aggregates.
3. **Feature store:** online features to Redis/Feast online store; offline features with point-in-time joins so training and serving cannot disagree.
4. **Scoring service:** the registered model scores each event from online features; every decision logged with its feature snapshot.
5. **Cloud deployment:** Terraform (or the VM path scaled up), autoscaling on the scoring service.
6. **Load test:** sustained events/second and p95 scoring latency; document where it breaks first.
7. **Safety + monitoring:** drift/data-quality alerts into the P5 dashboards; fallback to simple rules if the model service degrades.
8. **Write-up:** architecture + trade-offs — streaming vs batch, cost per 1k events, latency budget, the 10×-volume plan.

**Elective:** a LoRA/QLoRA fine-tune of a small open model for a narrow task, evaluated against the prompting baseline with the P5 harness.

## Done when

Live events are scored end-to-end on cloud infrastructure with measured throughput and latency, and the trade-off document exists.

## Stack

Python · Kafka/Bytewax · Feast/Redis · Terraform · Docker
