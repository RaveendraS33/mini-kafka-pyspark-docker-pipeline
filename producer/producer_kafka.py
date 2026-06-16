import argparse
import json
import logging
import os
import time

from kafka import KafkaProducer

from producer import add_bad_values, create_good_record


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")


def build_kafka_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda record: json.dumps(record).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
    )


def stream_records_to_kafka(
    topic,
    bootstrap_servers,
    total_batches,
    records_per_batch,
    bad_records_per_batch,
    sleep_seconds,
):
    producer = build_kafka_producer(bootstrap_servers)
    record_id = 1

    try:
        for batch_number in range(1, total_batches + 1):
            sent_count = 0

            for index in range(records_per_batch):
                record = create_good_record(record_id)

                if index < bad_records_per_batch:
                    record = add_bad_values(record)

                producer.send(
                    topic,
                    key=record["transaction_id"] or f"missing-key-{record_id}",
                    value=record,
                )
                record_id += 1
                sent_count += 1

            producer.flush()
            logger.info("Batch %s: sent %s records to topic '%s'", batch_number, sent_count, topic)
            time.sleep(sleep_seconds)
    finally:
        producer.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Publish synthetic transactions to Kafka.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--batches", type=int, default=int(os.getenv("PRODUCER_BATCHES", "10")))
    parser.add_argument("--records-per-batch", type=int, default=int(os.getenv("PRODUCER_RECORDS_PER_BATCH", "10")))
    parser.add_argument("--bad-records-per-batch", type=int, default=int(os.getenv("PRODUCER_BAD_RECORDS_PER_BATCH", "3")))
    parser.add_argument("--sleep-seconds", type=float, default=float(os.getenv("PRODUCER_SLEEP_SECONDS", "1")))
    return parser.parse_args()


def main():
    args = parse_args()
    stream_records_to_kafka(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        total_batches=args.batches,
        records_per_batch=args.records_per_batch,
        bad_records_per_batch=args.bad_records_per_batch,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()
