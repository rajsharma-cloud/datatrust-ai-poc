"""
Kafka -> BRONZE -> SILVER -> GOLD, continuously, in micro-batches.

    python streaming\\consumer.py          keeps running; Ctrl+C stops it cleanly

Every KAFKA_MICRO_BATCH_SECONDS (default 30 s) or KAFKA_MICRO_BATCH_MAX_EVENTS events:
  1. BRONZE : the events are appended to the same bronze tables the files use (load_mode = STREAM),
              one batch per business date, batch id K<yyyymmdd>_<hhmmss>. One transaction.
              Kafka offsets are committed only after bronze is saved (at-least-once: nothing is lost).
  2. SILVER : the existing run_silver() for that batch: same cleaning, rules, de-duplication, MERGE.
  3. GOLD   : the existing run_gold(): views refreshed and validated.
Everything is logged like a file run: audit.batch_control, audit.step_log, audit.error_log, plus
audit.kafka_ingest_log (topic, partition, offsets per batch). The web app refreshes by itself.

Messages that are not valid JSON go to the dead-letter topic and a WARNING in audit.error_log.
If Kafka or PostgreSQL is down, the consumer retries; it never stops the batch file pipeline.
"""
import json
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime

import kafka_config as kc
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

import db_utils                                     # from the project root (kafka_config adds it to the path)
from gold_layer import run_gold
from silver_layer import ENTITIES as SILVER_ENTITIES
from silver_layer import run_silver
from spark_utils import get_spark

META_COLUMNS = ["batch_id", "source_file", "ingested_at", "load_mode"]


def to_snake_case(name: str) -> str:              # same rule as bronze_layer.py: CUSTOMER_ID -> customer_id
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name.strip()).lower()


# ============================================================================ audit table for offsets
def ensure_tables() -> None:
    with db_utils.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit.kafka_ingest_log (
                ingest_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                batch_id       TEXT,
                business_date  DATE,
                topic          TEXT,
                kafka_partition INT,
                first_offset   BIGINT,
                last_offset    BIGINT,
                messages       INT,
                rows_landed    INT,
                bronze_table   TEXT,
                ingested_at    TIMESTAMP DEFAULT now())""")
        conn.commit()


# ============================================================================ bronze
def land_in_bronze(conn, batch_id: str, events: list, started_at: datetime) -> int:
    """events: [(topic, partition, offset, dict)] of ONE business date. Same transaction for all topics."""
    by_topic = defaultdict(list)
    for topic, partition, offset, event in events:
        by_topic[topic].append((partition, offset, {to_snake_case(k): v for k, v in event.items()}))
    total = 0
    for topic, items in by_topic.items():
        target = kc.TOPICS[topic]["bronze"]
        step_id = db_utils.pipeline_id("BRONZE", f"KAFKA_{kc.TOPICS[topic]['entity']}")
        with db_utils.track_step(batch_id, items[0][2].get("business_date"), "BRONZE", step_id) as info:
            fields = sorted({k for _, _, e in items for k in e})
            # bronze table: create if missing, add new fields as TEXT columns (schema drift is accepted, like files)
            schema, table = target.split(".")
            conn.execute(f"CREATE TABLE IF NOT EXISTS {target} ("
                         + ", ".join(f'"{c}" TEXT' for c in fields)
                         + ", batch_id TEXT, source_file TEXT, ingested_at TIMESTAMP, load_mode TEXT)")
            existing = {r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (schema, table)).fetchall()}
            new_fields = [c for c in fields if c not in existing]
            for c in new_fields:
                conn.execute(f'ALTER TABLE {target} ADD COLUMN "{c}" TEXT')
            columns = fields + META_COLUMNS
            now = datetime.now()
            with conn.cursor().copy(f"COPY {target} ({', '.join(chr(34) + c + chr(34) for c in columns)}) FROM STDIN") as cp:
                for _, _, e in items:
                    cp.write_row([None if e.get(c) in ("", None) else str(e.get(c)) for c in fields]
                                 + [batch_id, f"kafka:{topic}", now, "STREAM"])
            # offsets per partition -> audit.kafka_ingest_log (same transaction)
            parts = defaultdict(list)
            for partition, offset, _ in items:
                parts[partition].append(offset)
            for partition, offsets in parts.items():
                conn.execute("""INSERT INTO audit.kafka_ingest_log (batch_id, business_date, topic, kafka_partition,
                                       first_offset, last_offset, messages, rows_landed, bronze_table)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                             (batch_id, items[0][2].get("business_date"), topic, partition, min(offsets), max(offsets),
                              len(offsets), len(offsets), target))
            info.update(rows_in=len(items), rows_out=len(items),
                        message=f"{topic} -> {target}" + (f"; new columns: {', '.join(new_fields)}" if new_fields else ""))
            total += len(items)
            print(f"  bronze  {target:<24} +{len(items):>6} rows  (STREAM from {topic})")
    return total


# ============================================================================ silver helper
def snapshot_other_rejects(business_date: str, batch_id: str) -> list:
    """run_silver() replaces the rejected rows of a business date. With streaming, one date gets many
    micro-batches, so the rejects of the earlier batches of the same date are kept aside and put back."""
    with db_utils.get_connection() as conn:
        return conn.execute("""SELECT entity, record_key, business_date, rule_name, raw_record, batch_id, rejected_at
                               FROM silver.rejected_records
                               WHERE business_date = %s AND batch_id <> %s""", (business_date, batch_id)).fetchall()


def restore_rejects(rows: list) -> int:
    if not rows:
        return 0
    targets = {name: (table, key) for name, _, _, table, key, _ in SILVER_ENTITIES}
    restored = 0
    with db_utils.get_connection() as conn:
        for entity, record_key, day, rule, raw, batch, at in rows:
            table, key = targets.get(entity, (None, None))
            if table and conn.execute(f"SELECT 1 FROM {table} WHERE {key}::text = %s LIMIT 1", (record_key,)).fetchone():
                continue                                    # a later, correct version of the row is in silver now
            if conn.execute("""SELECT 1 FROM silver.rejected_records WHERE entity = %s AND record_key = %s
                               AND business_date = %s AND rule_name = %s""", (entity, record_key, day, rule)).fetchone():
                continue
            conn.execute("""INSERT INTO silver.rejected_records (entity, record_key, business_date, rule_name,
                            raw_record, batch_id, rejected_at) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                         (entity, record_key, day, rule, raw, batch, at))
            restored += 1
        conn.commit()
    return restored


# ============================================================================ one micro-batch
def process(spark, consumer, dlq, buffer: list, bad: list) -> None:
    """buffer: [(topic, partition, offset, event)], bad: [(topic, partition, offset, raw, error)]"""
    if bad:
        for topic, partition, offset, raw, error in bad:
            dlq.produce(kc.DEAD_LETTER_TOPIC, value=raw, headers={"source_topic": topic, "error": error[:200]})
        dlq.flush(10)
        db_utils.log_warning(None, None, "BRONZE", "PL_BRONZE_KAFKA_DEAD_LETTER", "DQ_BAD_MESSAGE",
                             f"{len(bad)} message(s) were not valid JSON and went to {kc.DEAD_LETTER_TOPIC}; "
                             f"first: {bad[0][0]} partition {bad[0][1]} offset {bad[0][2]}: {bad[0][4]}")

    by_date = defaultdict(list)
    for item in buffer:
        by_date[str(item[3].get("BUSINESS_DATE") or date.today().isoformat())].append(item)

    landed = []                                          # (batch_id, business_date) saved in bronze
    for day, events in sorted(by_date.items()):
        batch_id = f"K{day.replace('-', '')}_{datetime.now():%H%M%S}"
        started_at = datetime.now()
        print(f"\n=== stream batch {batch_id}  ({len(events)} events, business date {day})")
        conn = db_utils.get_connection()
        try:
            rows = land_in_bronze(conn, batch_id, events, started_at)
            conn.commit()                                # bronze saved
            db_utils.log_step(batch_id, day, "BRONZE", "SUCCESS", started_at, "STREAM",
                              rows_read=rows, rows_written=rows)
            landed.append((batch_id, day))
        except BaseException as e:
            conn.rollback()
            status = "CANCELLED" if db_utils.is_cancelled(e) else "FAILED"
            db_utils.log_error(batch_id, day, "BRONZE", "PL_BRONZE_KAFKA", e, status)
            db_utils.log_step(batch_id, day, "BRONZE", status, started_at, "STREAM", message=str(e)[:500])
            raise                                        # offsets NOT committed: the events are read again
        finally:
            conn.close()
        time.sleep(1)                                    # unique batch ids when one buffer has several dates

    consumer.commit(asynchronous=False)                  # bronze has everything -> move the offsets on
    buffer.clear()                                       # these events are saved: never land them twice
    bad.clear()

    if not kc.RUN_SILVER_GOLD:
        return
    for batch_id, day in landed:                         # silver / gold failures are logged by those layers;
        kept = snapshot_other_rejects(day, batch_id)     # the batch can be re-run: python silver_layer.py <batch>
        if run_silver(spark, batch_id, day):
            restore_rejects(kept)
            run_gold(batch_id, day)


# ============================================================================ main loop
def main():
    print(f"DataTrust Kafka consumer -> bronze/silver/gold   ({kc.BOOTSTRAP_SERVERS}, group {kc.GROUP_ID})")
    print(f"topics: {', '.join(kc.TOPICS)}   micro-batch: {kc.MICRO_BATCH_SECONDS}s or "
          f"{kc.MICRO_BATCH_MAX_EVENTS} events   Ctrl+C to stop")
    kc.check_connection()
    with db_utils.capture_errors("BRONZE", "PL_BRONZE_KAFKA_START"):
        ensure_tables()
        spark = get_spark("datatrust_kafka_consumer") if kc.RUN_SILVER_GOLD else None
        db_utils.install_stop_handler()                  # Ctrl+C inside Spark -> clean stop, logged CANCELLED
    consumer = Consumer(kc.client_config({"group.id": kc.GROUP_ID, "enable.auto.commit": False,
                                          "auto.offset.reset": "earliest"}))
    dlq = Producer(kc.client_config())
    consumer.subscribe(list(kc.TOPICS))

    buffer, bad = [], []
    first_offsets = {}                                   # (topic, partition) -> first offset in the buffer
    deadline = time.time() + kc.MICRO_BATCH_SECONDS
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"  kafka: {msg.error()}")
                        if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:   # topics appear with 1st event
                            db_utils.log_error(None, None, "BRONZE", "PL_BRONZE_KAFKA_POLL", KafkaException(msg.error()))
                else:
                    topic, partition, offset = msg.topic(), msg.partition() or 0, msg.offset() or 0
                    first_offsets.setdefault((topic, partition), offset)
                    try:
                        event = json.loads(msg.value())
                        if not isinstance(event, dict):
                            raise ValueError("message is not a JSON object")
                        buffer.append((topic, partition, offset, event))
                    except (ValueError, TypeError) as e:
                        bad.append((topic, partition, offset, msg.value(), str(e)))

            if (buffer or bad) and (time.time() >= deadline or len(buffer) >= kc.MICRO_BATCH_MAX_EVENTS):
                try:
                    process(spark, consumer, dlq, buffer, bad)
                except KeyboardInterrupt:
                    raise
                except Exception as e:                   # PostgreSQL down, etc.: read the same events again later
                    print(f"  batch not saved ({type(e).__name__}: {e}); retrying in 30 s")
                    for (topic, partition), offset in first_offsets.items():
                        consumer.seek(TopicPartition(topic, partition, offset))
                    time.sleep(30)
                buffer, bad, first_offsets = [], [], {}
                deadline = time.time() + kc.MICRO_BATCH_SECONDS
            elif time.time() >= deadline:
                deadline = time.time() + kc.MICRO_BATCH_SECONDS
    except KeyboardInterrupt:
        print("\nstopping ...")
        if buffer or bad:
            try:
                db_utils.STOP_REQUESTED = False
                process(spark, consumer, dlq, buffer, bad)  # do not lose what was already read
            except BaseException as e:
                print(f"  last batch not saved ({type(e).__name__}); it will be read again next start")
    finally:
        consumer.close()
        if spark is not None:
            spark.stop()
        print("consumer stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
