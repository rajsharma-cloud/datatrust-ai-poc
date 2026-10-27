"""
Create the DataTrust topics (safe to run again) and check the connection to Kafka.

    python streaming\\create_topics.py
"""
import sys

import kafka_config as kc
from confluent_kafka.admin import AdminClient, NewTopic


def main() -> int:
    admin = AdminClient(kc.client_config())
    try:
        existing = admin.list_topics(timeout=10).topics
    except Exception as e:
        print(f"cannot reach Kafka at {kc.BOOTSTRAP_SERVERS}: {e}\n"
              "Is it running?  docker compose -f streaming\\docker-compose.yml up -d")
        return 1
    print(f"connected to {kc.BOOTSTRAP_SERVERS}")
    wanted = list(kc.TOPICS) + [kc.DEAD_LETTER_TOPIC]
    new = [NewTopic(t, num_partitions=kc.TOPIC_PARTITIONS, replication_factor=1,
                    config={"retention.ms": str(7 * 24 * 3600 * 1000)})        # keep events 7 days (replay)
           for t in wanted if t not in existing]
    if new:
        for topic, future in admin.create_topics(new).items():
            try:
                future.result()
                print(f"  created {topic} ({kc.TOPIC_PARTITIONS} partitions)")
            except Exception as e:
                print(f"  {topic}: {e}")
    for t in wanted:
        if t in existing:
            print(f"  exists  {t}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
