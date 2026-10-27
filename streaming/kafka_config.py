"""
Kafka settings for the DataTrust streaming add-on. Values come from the project's .env file;
the defaults below work with the local Kafka started by streaming/docker-compose.yml.

The same code works with a managed Kafka (Confluent Cloud, Azure Event Hubs Kafka endpoint, AWS MSK)
by changing only the .env values (bootstrap server + SASL user/password).
"""
import os
import sys
from pathlib import Path


def _project_root() -> Path:
    for folder in Path(__file__).resolve().parents:
        if (folder / "config.py").exists():
            return folder
    raise SystemExit("config.py not found: put the 'streaming' folder inside your project folder, next to config.py")


PROJECT_DIR = _project_root()
sys.path.insert(0, str(PROJECT_DIR))                     # so config, db_utils, silver_layer ... can be imported
import config  # noqa: E402,F401  (loads .env)

# ---------------------------------------------------------------- connection
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")   # SASL_SSL for Confluent Cloud / Event Hubs
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")                  # Event Hubs: $ConnectionString
SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")                  # Event Hubs: the connection string

# ---------------------------------------------------------------- consumer behaviour
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "datatrust-bronze-loader")
MICRO_BATCH_SECONDS = int(os.getenv("KAFKA_MICRO_BATCH_SECONDS", "30"))    # land + process every N seconds
MICRO_BATCH_MAX_EVENTS = int(os.getenv("KAFKA_MICRO_BATCH_MAX_EVENTS", "5000"))
RUN_SILVER_GOLD = os.getenv("KAFKA_RUN_SILVER_GOLD", "true").lower() == "true"   # false = land in bronze only

# ---------------------------------------------------------------- topics
# topic -> where its events land. Field names in the messages are the same as the CSV headers,
# so streamed rows and file rows end up in the same bronze tables and go through the same silver rules.
TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "datatrust")
TOPICS = {
    f"{TOPIC_PREFIX}.payment_orders": {"entity": "payment_orders", "bronze": "bronze.payment_orders", "key": "ORDER_ID"},
    f"{TOPIC_PREFIX}.payments":       {"entity": "payments",       "bronze": "bronze.payments",       "key": "PAYMENT_ID"},
    f"{TOPIC_PREFIX}.refunds":        {"entity": "refunds",        "bronze": "bronze.refunds",        "key": "REFUND_ID"},
}
DEAD_LETTER_TOPIC = f"{TOPIC_PREFIX}.dead_letter"          # messages that are not valid JSON go here
TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))


def client_config(extra: dict = None) -> dict:
    """librdkafka settings shared by producer, consumer and admin client."""
    conf = {"bootstrap.servers": BOOTSTRAP_SERVERS, "client.id": "datatrust-poc",
            "log_level": 2}                    # quiet librdkafka; connection problems are reported by our code
    if SECURITY_PROTOCOL != "PLAINTEXT":
        conf.update({"security.protocol": SECURITY_PROTOCOL, "sasl.mechanisms": SASL_MECHANISM,
                     "sasl.username": SASL_USERNAME, "sasl.password": SASL_PASSWORD})
    conf.update(extra or {})
    return conf


def check_connection(timeout: float = 10) -> None:
    """Stop with a clear message if Kafka cannot be reached."""
    from confluent_kafka.admin import AdminClient
    try:
        AdminClient(client_config()).list_topics(timeout=timeout)
    except Exception as e:
        raise SystemExit(f"cannot reach Kafka at {BOOTSTRAP_SERVERS} ({e}).\n"
                         "Start it first:  docker compose -f streaming\\docker-compose.yml up -d")
