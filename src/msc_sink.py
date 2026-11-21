"""
msc_sink.py — mongo-stream-cdc
Kafka → Postgres

- Consumes from all msc.* topics (one per collection)
- Schema-aware upsert using combined_table_config_3.json
- Manual offset commit — only after successful Postgres write
- Handles inserts, updates, replaces, deletes
- SASL/PLAIN authentication (no SSL) for Kafka
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
import certifi

import psycopg2
from psycopg2.extras import execute_batch
from confluent_kafka import Consumer, KafkaException, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [msc_sink] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


# ── Config ────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)

def load_table_config(path: str) -> list:
    with open(path) as f:
        return json.load(f)


# ── Kafka Auth ────────────────────────────────────────────────────

def kafka_base_config() -> dict:
    username = os.environ.get("KAFKA_USERNAME", "")
    password = os.environ.get("KAFKA_PASSWORD", "")
    cfg = {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "ssl.endpoint.identification.algorithm": "none",
        "ssl.ca.location":   certifi.where(),
        "socket.timeout.ms":         30000,
        "message.max.bytes":         10485760,
        "fetch.message.max.bytes":   10485760,
    }
    if username:
        cfg["sasl.username"] = username
        cfg["sasl.password"] = password
    return cfg


# ── Schema Registry ───────────────────────────────────────────────

def build_schema_registry(table_config: list) -> dict:
    registry = {}
    for tc in table_config:
        collection    = tc["table_name"]
        pg_table      = tc.get("metabase_table_name", collection)
        schema_fields = tc["schema"]
        columns       = _extract_columns(schema_fields)
        schema_map    = _build_schema_map(schema_fields)

        registry[collection] = {
            "pg_table":   pg_table,
            "columns":    columns,
            "schema_map": schema_map,
            "upsert_sql": _build_upsert_sql(pg_table, columns),
            "delete_sql": f"DELETE FROM {pg_table} WHERE id = %s",
        }
    return registry

def _extract_columns(schema_fields: list) -> list:
    columns = []
    for field in schema_fields:
        field = field.strip()
        if field.upper().startswith("PRIMARY KEY"):
            continue
        columns.append(field.split()[0])
    return columns

def _build_schema_map(schema_fields: list) -> dict:
    schema_map = {}
    for field in schema_fields:
        field = field.strip()
        if field.upper().startswith("PRIMARY KEY"):
            continue
        parts = field.split(None, 1)
        if len(parts) == 2:
            schema_map[parts[0]] = parts[1].lower()
    return schema_map

def _build_upsert_sql(pg_table: str, columns: list) -> str:
    placeholders = ", ".join(["%s"] * len(columns))
    col_list     = ", ".join(columns)
    update_cols  = [c for c in columns if c != "id"]
    set_clause   = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    return (
        f"INSERT INTO {pg_table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {set_clause}"
    )


# ── Type Coercion ─────────────────────────────────────────────────

def coerce_value(value, col_name: str, schema_map: dict):
    if value is None:
        return None

    col_type = schema_map.get(col_name, "")

    if "jsonb" in col_type or "json" in col_type:
        if value == "" or value == b"":   # empty string → NULL for jsonb
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, str):
            try:
                json.loads(value)         # already valid JSON string (e.g. '{}')
                return value
            except (ValueError, TypeError):
                return None               # unparseable string → NULL
        return json.dumps(value)

    if "uuid" in col_type:
        return str(value) if value else None

    if "boolean" in col_type or "bool" in col_type:
        if isinstance(value, bool):
            return value
        return str(value).lower() in ("true", "1", "yes")

    if col_type in ("integer", "bigint", "int"):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    if col_type in ("float8", "float", "numeric", "real"):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    return str(value) if not isinstance(value, str) else value


# ── Postgres ──────────────────────────────────────────────────────

def get_pg_conn(postgres_uri: str):
    return psycopg2.connect(postgres_uri)

def ensure_tables_exist(pg_conn, table_config: list):
    with pg_conn.cursor() as cur:
        for tc in table_config:
            pg_table      = tc.get("metabase_table_name", tc["table_name"])
            schema_fields = tc["schema"]
            schema_str    = ",\n  ".join(schema_fields)
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {pg_table} (\n  {schema_str}\n)"
            )
    pg_conn.commit()
    logging.info("All tables verified in Postgres")


def ensure_id_map_table(pg_conn):
    """
    MongoDB delete events only carry documentKey._id, which for many
    collections is a raw ObjectId — not the same as the UUID `id` column
    used as the Postgres primary key. This table remembers the mapping
    from mongo_id -> id, learned from every insert/update, so deletes
    can resolve the correct primary key without ever touching MongoDB.
    """
    with pg_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS msc_mongo_id_map (
                collection text NOT NULL,
                mongo_id   text NOT NULL,
                id         text NOT NULL,
                updated_at timestamp DEFAULT now(),
                PRIMARY KEY (collection, mongo_id)
            )
        """)
    pg_conn.commit()
    logging.info("msc_mongo_id_map table verified")


# ── Message Processing ────────────────────────────────────────────

def process_message(pg_conn, payload: dict, schema_registry: dict):
    collection = payload.get("collection")
    operation  = payload.get("operation")
    doc_id     = payload.get("document_id")   # mongo _id — ObjectId hex or UUID str
    document   = payload.get("document", {})

    schema = schema_registry.get(collection)
    if not schema:
        logging.warning("No schema for collection: %s — skipping", collection)
        return

    with pg_conn.cursor() as cur:
        if operation in ("insert", "update", "replace"):
            row = tuple(
                coerce_value(document.get(col), col, schema["schema_map"])
                for col in schema["columns"]
            )
            cur.execute(schema["upsert_sql"], row)

            # Learn mongo_id -> id mapping for future deletes
            if "id" in schema["columns"]:
                pg_id = row[schema["columns"].index("id")]
                if pg_id:
                    cur.execute(
                        """INSERT INTO msc_mongo_id_map (collection, mongo_id, id)
                           VALUES (%s, %s, %s)
                           ON CONFLICT (collection, mongo_id)
                           DO UPDATE SET id = EXCLUDED.id, updated_at = now()""",
                        (collection, doc_id, pg_id)
                    )

        elif operation == "delete":
            cur.execute(
                "SELECT id FROM msc_mongo_id_map WHERE collection = %s AND mongo_id = %s",
                (collection, doc_id)
            )
            mapped      = cur.fetchone()
            delete_key  = mapped[0] if mapped else doc_id

            try:
                uuid.UUID(str(delete_key))
                valid_uuid = True
            except (ValueError, AttributeError, TypeError):
                valid_uuid = False

            if not valid_uuid:
                logging.warning(
                    "[%s] delete doc_id=%s — no id mapping and doc_id is not a "
                    "UUID; skipping (row was likely never synced via CDC)",
                    collection, doc_id
                )
            else:
                cur.execute(schema["delete_sql"], (delete_key,))
                cur.execute(
                    "DELETE FROM msc_mongo_id_map WHERE collection = %s AND mongo_id = %s",
                    (collection, doc_id)
                )

        else:
            logging.warning("Unknown operation: %s — skipping", operation)
            return

    pg_conn.commit()
    logging.info("[%s] %s doc_id=%s", collection, operation, doc_id)


# ── Kafka Consumer ────────────────────────────────────────────────

def make_consumer(config: dict) -> Consumer:
    cfg = kafka_base_config()
    cfg.update({
        "bootstrap.servers":    ",".join(config["kafka_bootstrap_servers"]),
        "group.id":             config["kafka_consumer_group"],
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,   # manual commit after Postgres write
        "session.timeout.ms":   30000,
        "max.poll.interval.ms": 300000,
    })
    return Consumer(cfg)


def build_topic_list(config: dict, table_names: list) -> list:
    prefix          = config.get("kafka_topic_prefix", "msc")
    seen            = set()
    topics          = []
    skipped_invalid = []
    skipped_dupes   = []

    for name in table_names:
        name = (name or "").strip()
        if not name:
            skipped_invalid.append(repr(name))
            continue

        topic = f"{prefix}.{name.lower()}"

        if topic in seen:
            skipped_dupes.append(topic)
            continue

        seen.add(topic)
        topics.append(topic)

    if skipped_invalid:
        logging.warning("Skipped invalid (empty) table names: %s", skipped_invalid)
    if skipped_dupes:
        logging.warning("Skipped duplicate topics: %s", skipped_dupes)

    logging.info("Subscribing to %d unique topics", len(topics))
    return topics


# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="mongo-stream-cdc: Kafka → Postgres sink (msc_sink)"
    )
    parser.add_argument("--config",       required=True)
    parser.add_argument("--table-config", required=True)
    args = parser.parse_args()

    config       = load_config(args.config)
    table_config = load_table_config(args.table_config)

    postgres_uri    = os.environ["POSTGRES_URI"]
    schema_registry = build_schema_registry(table_config)
    table_names     = [t["table_name"] for t in table_config]
    topics          = build_topic_list(config, table_names)

    if not topics:
        logging.error("No valid topics to subscribe to — check table config")
        sys.exit(1)

    pg_conn  = get_pg_conn(postgres_uri)
    ensure_tables_exist(pg_conn, table_config)
    ensure_id_map_table(pg_conn)
    consumer = make_consumer(config)

    try:
        consumer.subscribe(topics)
        logging.info("Subscribed successfully to %d topics", len(topics))
    except KafkaException as e:
        logging.error("Failed to subscribe to topics: %s", e)
        logging.error("Topics attempted: %s", topics)
        sys.exit(1)

    while True:
        try:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            payload = json.loads(msg.value().decode("utf-8"))

            try:
                process_message(pg_conn, payload, schema_registry)
                consumer.commit(message=msg)

            except psycopg2.Error as e:
                logging.error("Postgres error: %s — rolling back, will retry", e)
                pg_conn.rollback()
                try:
                    pg_conn = get_pg_conn(postgres_uri)
                except Exception as reconnect_err:
                    logging.error("Failed to reconnect Postgres: %s", reconnect_err)
                time.sleep(2)

            except Exception as e:
                logging.error("Failed to process message: %s — skipping", e)
                consumer.commit(message=msg)

        except KafkaException as e:
            logging.error("Kafka error: %s — retrying in 5s", e)
            time.sleep(5)

        except Exception as e:
            logging.error("Unexpected error: %s — retrying in 5s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
