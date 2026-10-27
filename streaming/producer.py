"""
Payment gateway simulator: streams live order, payment and refund events to Kafka.

Each event looks exactly like a row of the source CSV files (same field names), built from real
rows of data/source_csv as templates, with new IDs, today's business date and current timestamps.

    python streaming\\producer.py                                   5 orders/sec until Ctrl+C
    python streaming\\producer.py --rate 20 --minutes 2            20 orders/sec for 2 minutes
    python streaming\\producer.py --date 2026-10-09 --count 1500   one more business day, 1,500 orders

Inject a data incident (for demos) into a share of the events:
    --inject missing_currency --pct 2     payments without currency   -> silver rejects them
    --inject negative_amount  --pct 1     orders with a negative amount -> silver rejects them
    --inject duplicates       --pct 3     the same payment sent twice  -> silver removes the copy
    --inject bad_json         --pct 1     unreadable messages          -> dead-letter topic + warning
"""
import argparse
import csv
import json
import random
import signal
import time
from datetime import date, datetime, timedelta

import kafka_config as kc
from confluent_kafka import Producer

STOP = False


def load_templates():
    """Real rows from the source files, used as realistic templates (methods, banks, amounts, customers)."""
    folder = kc.PROJECT_DIR / "data" / "source_csv"
    read = lambda f: list(csv.DictReader(open(folder / f, newline="", encoding="utf-8")))   # noqa: E731
    orders, payments, refunds = read("payment_orders.csv"), read("payments.csv"), read("refunds.csv")
    return {
        "orders": orders,
        "captured": [p for p in payments if p["STATUS"] == "CAPTURED" and p["CURRENCY"]],
        "failed": [p for p in payments if p["STATUS"] == "FAILED"],
        "refunds": refunds,
    }


class Gateway:
    def __init__(self, business_date: str, inject: str, pct: float):
        self.t = load_templates()
        self.day = business_date
        self.inject, self.pct = inject, pct
        self.run_id = datetime.now().strftime("%H%M%S")          # new IDs on every producer start
        self.seq = 0
        self.stats = {"orders": 0, "payments": 0, "refunds": 0, "injected": 0}

    def _now(self, plus_seconds: int = 0) -> str:
        clock = datetime.now() + timedelta(seconds=plus_seconds)
        return f"{self.day} {clock:%H:%M:%S}"                    # business date + live time of day

    def _hit(self) -> bool:
        if self.inject != "none" and random.random() * 100 < self.pct:
            self.stats["injected"] += 1
            return True
        return False

    def next_events(self):
        """One customer order -> order event, payment event(s), sometimes a refund."""
        self.seq += 1
        tag = f"{self.day.replace('-', '')}{self.run_id}{self.seq:05d}"
        o = dict(random.choice(self.t["orders"]))
        completed = random.random() > 0.03
        order_id = f"KORD{tag}"
        o.update(ORDER_ID=order_id, RECEIPT=f"rcpt_{order_id}", BUSINESS_DATE=self.day, CREATED_AT=self._now(),
                 STATUS="PAID" if completed else "CREATED", ORDER_STATUS="COMPLETED" if completed else "CANCELLED",
                 AMOUNT_PAID=o["AMOUNT"] if completed else "0.00", AMOUNT_DUE="0.00" if completed else o["AMOUNT"])
        if self.inject == "negative_amount" and self._hit():
            o["AMOUNT"] = "-" + o["AMOUNT"].lstrip("-")
        events = [("payment_orders", order_id, o)]
        self.stats["orders"] += 1

        p = dict(random.choice(self.t["captured"] if completed else self.t["failed"]))
        payment_id = f"KPAY{tag}"
        p.update(PAYMENT_ID=payment_id, ORDER_ID=order_id, CUSTOMER_ID=o["CUSTOMER_ID"], BUSINESS_DATE=self.day,
                 CREATED_AT=self._now(), CAPTURED_AT=self._now(4) if completed else "", AMOUNT=o["AMOUNT"].lstrip("-"),
                 RRN=str(random.randint(10**11, 10**12 - 1)))
        if self.inject == "missing_currency" and completed and self._hit():
            p["CURRENCY"] = ""
        events.append(("payments", order_id, p))
        if self.inject == "duplicates" and self._hit():
            events.append(("payments", order_id, dict(p)))          # same payment delivered twice
        self.stats["payments"] += 1

        if completed and random.random() < 0.015:                   # ~1.5% of payments are refunded
            r = dict(random.choice(self.t["refunds"]))
            r.update(REFUND_ID=f"KREF{tag}", PAYMENT_ID=payment_id, ORDER_ID=order_id, BUSINESS_DATE=self.day,
                     CREATED_AT=self._now(30), PROCESSED_AT=self._now(60), AMOUNT=p["AMOUNT"])
            events.append(("refunds", order_id, r))
            self.stats["refunds"] += 1
        return events


def topic_for(entity: str) -> str:
    return next(t for t, v in kc.TOPICS.items() if v["entity"] == entity)


def main():
    ap = argparse.ArgumentParser(description="Stream payment events to Kafka")
    ap.add_argument("--rate", type=float, default=5, help="orders per second (default 5)")
    ap.add_argument("--date", default=date.today().isoformat(), help="business date of the events (default today)")
    ap.add_argument("--minutes", type=float, help="stop after N minutes")
    ap.add_argument("--count", type=int, help="stop after N orders")
    ap.add_argument("--inject", default="none",
                    choices=["none", "missing_currency", "negative_amount", "duplicates", "bad_json"])
    ap.add_argument("--pct", type=float, default=2.0, help="percentage of events affected by --inject")
    args = ap.parse_args()
    date.fromisoformat(args.date)                                    # invalid date -> clear error

    kc.check_connection()
    producer = Producer(kc.client_config({"linger.ms": 50, "acks": "all", "enable.idempotence": True}))
    gw = Gateway(args.date, args.inject, args.pct)
    failures = []

    def delivered(err, msg):
        if err is not None:
            failures.append(str(err))

    def stop(*_):
        global STOP
        STOP = True
    signal.signal(signal.SIGINT, stop)

    print(f"streaming to {kc.BOOTSTRAP_SERVERS}: {args.rate} orders/sec, business date {args.date}, "
          f"inject={args.inject}{f' ({args.pct}%)' if args.inject != 'none' else ''}   Ctrl+C to stop")
    started = time.time()
    last_print = started
    while not STOP:
        if args.count and gw.stats["orders"] >= args.count:
            break
        if args.minutes and time.time() - started >= args.minutes * 60:
            break
        for entity, key, event in gw.next_events():
            value = json.dumps(event)
            if args.inject == "bad_json" and gw._hit():
                value = value[: len(value) // 2]                       # truncated message
            producer.produce(topic_for(entity), key=key, value=value, on_delivery=delivered)
        producer.poll(0)
        if time.time() - last_print >= 5:
            last_print = time.time()
            print(f"  sent {gw.stats['orders']:,} orders, {gw.stats['payments']:,} payments, "
                  f"{gw.stats['refunds']:,} refunds, {gw.stats['injected']} injected"
                  + (f", {len(failures)} delivery errors (last: {failures[-1]})" if failures else ""))
        time.sleep(1.0 / args.rate)

    left = producer.flush(15)
    print(f"done: {gw.stats}  delivery errors: {len(failures)}  not delivered: {left}")
    if failures:
        print("first delivery error:", failures[0])


if __name__ == "__main__":
    main()
