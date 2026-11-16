"""Producer CLI: emit synthetic transaction events.

Default mode writes JSONL to stdout or a file — no broker needed.
Kafka mode (``--kafka``) publishes to a Kafka-compatible broker
(Redpanda locally, see docker-compose.yml) and is the only mode that
needs the optional ``kafka-python`` dependency
(``pip install -r requirements-kafka.txt``).

Examples:
    python -m streaming_fraud.producer --n 100                 # to stdout
    python -m streaming_fraud.producer --n 100 -o events.jsonl # to file
    python -m streaming_fraud.producer --kafka --n 100         # to Redpanda
"""

from __future__ import annotations

import argparse
import sys

from .events import events_to_jsonl, generate_events


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=100, help="number of events")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "-o", "--output", default="-", help="output file, or - for stdout"
    )
    parser.add_argument(
        "--kafka",
        action="store_true",
        help="publish to a Kafka-compatible broker instead of writing JSONL",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="transactions")
    args = parser.parse_args(argv)

    events = generate_events(n=args.n, seed=args.seed)

    if args.kafka:
        try:
            from kafka import KafkaProducer
        except ImportError:
            print(
                "kafka-python is not installed. Run: "
                "pip install -r requirements-kafka.txt",
                file=sys.stderr,
            )
            return 2
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: v.encode("utf-8"),
        )
        for event in events:
            producer.send(args.topic, key=event.card_id.encode(), value=event.to_json())
        producer.flush()
        producer.close()
        print(
            f"published {len(events)} events to topic '{args.topic}' "
            f"at {args.bootstrap_servers}",
            file=sys.stderr,
        )
        return 0

    text = events_to_jsonl(events)
    if args.output == "-":
        sys.stdout.write(text)
    else:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text)
        print(f"wrote {len(events)} events to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
