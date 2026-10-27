# DataTrust streaming add-on (Kafka)

Live payment events flow **Kafka → Bronze → Silver → Gold** every 30 seconds, through the same bronze
tables, silver rules and gold views as the CSV files. The batch pipeline, run_pipeline.py and the web
app keep working unchanged; the web page refreshes itself as each micro-batch lands.

```
producer.py (payment gateway simulator)                  consumer.py (runs continuously)
  datatrust.payment_orders  ─┐                             every 30 s / 5,000 events:
  datatrust.payments        ─┼──►  Kafka  ──────────────►   BRONZE  append (load_mode STREAM), commit offsets
  datatrust.refunds         ─┘     (bad JSON ► datatrust.dead_letter)    SILVER  run_silver()  (rules, dedupe, MERGE)
                                                             GOLD    run_gold()    (views + validation)
                                                             audit.* logs + audit.kafka_ingest_log (offsets)
```

## Files
| File | Purpose |
|---|---|
| kafka_config.py | All Kafka settings (read from .env): server, topics → bronze tables, micro-batch size, SASL |
| docker-compose.yml | Local Kafka 3.8 (KRaft, no ZooKeeper) + Kafka UI on http://localhost:8080 |
| create_topics.py | Creates the topics, checks the connection |
| producer.py | Streams realistic order / payment / refund events; can inject incidents |
| consumer.py | Kafka → bronze → silver → gold, continuously |

## One-time setup
1. Copy the `streaming` folder into the project folder (next to config.py).
2. `pip install confluent-kafka`
3. Kafka needs Docker on Windows. Docker Desktop needs a paid licence in large companies; the free
   alternatives Rancher Desktop or Podman Desktop work with the same `docker compose` command.
   ```powershell
   docker compose -f streaming\docker-compose.yml up -d
   python streaming\create_topics.py
   ```

## Run (3 terminals, venv activated in each)
| Terminal | Command |
|---|---|
| 1 | `python web\server.py` (web app, already running) |
| 2 | `python streaming\consumer.py` (leave running) |
| 3 | `python streaming\producer.py` (5 orders/sec, today's date, Ctrl+C to stop) |

Ask the web agent "revenue today" and watch it grow every 30 seconds.

## Demo: inject an incident live
```powershell
python streaming\producer.py --inject missing_currency --pct 5 --minutes 2   # silver rejects, trust score drops
python streaming\producer.py --inject duplicates --pct 3 --minutes 2         # silver removes copies
python streaming\producer.py --inject negative_amount --pct 2 --minutes 2    # rule AMOUNT_POSITIVE
python streaming\producer.py --inject bad_json --pct 1 --minutes 1           # dead-letter topic + WARNING
python streaming\producer.py --date 2026-10-09 --count 1500                  # one more full business day
```

## Guarantees
* **No data loss**: offsets are committed only after the events are saved in bronze. If PostgreSQL is
  down, the consumer re-reads the same events and retries every 30 s.
* **No double counting**: if events are delivered twice (at-least-once), silver merges on the business key.
* **Silver/gold failure**: logged like a file run; the events are already safe in bronze, re-run with
  `python silver_layer.py <batch_id>` then `python gold_layer.py <batch_id>`.
* **Ctrl+C**: the consumer saves what it has already read, then stops.
* Batch ids start with **K** (e.g. K20261020_114530); bronze load_mode = STREAM; offsets per partition
  in `audit.kafka_ingest_log`.

## Managed Kafka (no code change)
Set in .env: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL=SASL_SSL`, `KAFKA_SASL_MECHANISM=PLAIN`,
`KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`.
Azure Event Hubs: server `<namespace>.servicebus.windows.net:9093`, username `$ConnectionString`,
password = the namespace connection string (Standard tier or higher; create the topics as event hubs).
