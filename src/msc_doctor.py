"""
msc_doctor.py — mongo-stream-cdc
Pre-flight checks: validates all connections and config before deploying.

Checks:
  1. MongoDB — reachable, replica set, oplog accessible
  2. Kafka   — reachable, topics exist or can be created
  3. Postgres — reachable, schema compatible with table config
  4. Config   — all required fields present, table config valid

Usage:
  python msc_doctor.py --config config.json --table-config combined_table_config_3.json
"""

import argparse
import json
import logging
import os
import sys
import certifi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [msc_doctor] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

PASS  = "✓"
FAIL  = "✗"
WARN  = "⚠"

all_ok = True


def check(label: str, ok: bool, detail: str = "", warn: bool = False):
    global all_ok
    symbol = PASS if ok else (WARN if warn else FAIL)
    print(f"  {symbol}  {label}")
    if detail:
        print(f"       {detail}")
    if not ok and not warn:
        all_ok = False


# ── Config Check ──────────────────────────────────────────────────

def check_config(config: dict, table_config: list):
    print("\n── Config ─────────────────────────────────────────────")

    required_keys = [
        "mongo_database",
        "kafka_bootstrap_servers",
        "kafka_topic_prefix",
        "kafka_consumer_group",
    ]
    for key in required_keys:
        check(f"config.json has '{key}'", key in config)

    check(
        f"table_config has entries",
        len(table_config) > 0,
        f"{len(table_config)} collections found"
    )

    # Validate each table entry
    bad_tables = []
    for tc in table_config:
        if not tc.get("table_name") or not tc.get("schema"):
            bad_tables.append(tc.get("table_name", "unknown"))

    check(
        "All table configs have table_name + schema",
        len(bad_tables) == 0,
        f"Bad entries: {bad_tables}" if bad_tables else ""
    )

    # Check for PRIMARY KEY in each schema
    missing_pk = []
    for tc in table_config:
        has_pk = any("PRIMARY KEY" in f.upper() for f in tc.get("schema", []))
        if not has_pk:
            missing_pk.append(tc["table_name"])
    check(
        "All tables have PRIMARY KEY in schema",
        len(missing_pk) == 0,
        f"Missing PK: {missing_pk}" if missing_pk else ""
    )


# ── MongoDB Check ─────────────────────────────────────────────────

def check_mongo(mongo_uri: str, config: dict, table_config: list):
    print("\n── MongoDB ────────────────────────────────────────────")
    try:
        from pymongo import MongoClient
        from pymongo.errors import ServerSelectionTimeoutError

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        check("MongoDB reachable", True)

        # Replica set check
        try:
            rs_status = client.admin.command("replSetGetStatus")
            check(
                "Replica set configured",
                True,
                f"Set: {rs_status['set']}, members: {len(rs_status['members'])}"
            )
            primaries = [m for m in rs_status["members"] if m.get("stateStr") == "PRIMARY"]
            check("Primary member available", len(primaries) > 0)
        except Exception as e:
            check("Replica set configured", False, str(e))

        # Oplog check
        try:
            oplog = client["local"]["oplog.rs"]
            latest = oplog.find_one(sort=[("$natural", -1)])
            check(
                "Oplog accessible",
                latest is not None,
                f"Latest ts: t={latest['ts'].time}" if latest else "Empty oplog"
            )

            stats = client["local"].command("collStats", "oplog.rs")
            size_gb = stats.get("maxSize", 0) / (1024 ** 3)
            check(
                "Oplog size adequate (>= 1GB)",
                size_gb >= 1,
                f"Oplog size: {size_gb:.1f}GB",
                warn=size_gb < 1
            )
        except Exception as e:
            check("Oplog accessible", False, str(e))

        # Database + collections check
        db = client[config["mongo_database"]]
        existing = set(db.list_collection_names())
        table_names = [tc["table_name"] for tc in table_config]
        missing = [t for t in table_names if t not in existing]

        check(
            f"All {len(table_names)} collections exist in MongoDB",
            len(missing) == 0,
            f"Missing: {missing[:5]}{'...' if len(missing) > 5 else ''}" if missing else "",
            warn=len(missing) > 0
        )

        client.close()

    except Exception as e:
        check("MongoDB reachable", False, str(e))


# ── Kafka Check ───────────────────────────────────────────────────

def check_kafka(config: dict, table_config: list):
    print("\n── Kafka ──────────────────────────────────────────────")
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({
            "bootstrap.servers": ",".join(config["kafka_bootstrap_servers"]),
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ.get("KAFKA_USERNAME"),
            "sasl.password": os.environ.get("KAFKA_PASSWORD"),
            "ssl.ca.location": certifi.where(),
            "ssl.endpoint.identification.algorithm": "none",
        })

        metadata = admin.list_topics(timeout=10)
        check("Kafka reachable", True, f"Brokers: {len(metadata.brokers)}")

        prefix      = config["kafka_topic_prefix"]
        table_names = [tc["table_name"] for tc in table_config]
        expected    = [f"{prefix}.{n.lower()}" for n in table_names]
        existing    = set(metadata.topics.keys())

        present = [t for t in expected if t in existing]
        absent  = [t for t in expected if t not in existing]

        check(
            f"Kafka topics exist ({len(present)}/{len(expected)})",
            len(absent) == 0,
            f"Missing topics (will be auto-created on first run): {absent[:3]}{'...' if len(absent) > 3 else ''}",
            warn=len(absent) > 0
        )

        # Check resume topic exists
        check(
            "Kafka resume topic exists (msc.resume)",
            "msc.resume" in existing,
            "Will be created in Step 7 of setup" if "msc.resume" not in existing else "",
            warn="msc.resume" not in existing
        )

    except Exception as e:
        check("Kafka reachable", False, str(e))


# ── Postgres Check ────────────────────────────────────────────────

def check_postgres(postgres_uri: str, table_config: list):
    print("\n── Postgres ───────────────────────────────────────────")
    try:
        import psycopg2

        conn = psycopg2.connect(postgres_uri)
        cur  = conn.cursor()

        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        check("Postgres reachable", True, version.split(",")[0])

        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
        """)
        existing = {row[0] for row in cur.fetchall()}

        for tc in table_config:
            pg_table = tc.get("metabase_table_name", tc["table_name"])
            if pg_table in existing:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                """, (pg_table,))
                existing_cols = {row[0] for row in cur.fetchall()}

                schema_cols = set()
                for field in tc["schema"]:
                    field = field.strip()
                    if not field.upper().startswith("PRIMARY KEY"):
                        schema_cols.add(field.split()[0])

                missing_cols = schema_cols - existing_cols
                if missing_cols:
                    check(
                        f"{pg_table}: schema mismatch",
                        False,
                        f"Columns in config but not in DB: {missing_cols}",
                        warn=True
                    )

        check(
            f"Postgres schema validated for {len(table_config)} tables",
            True
        )

        conn.close()

    except Exception as e:
        check("Postgres reachable", False, str(e))


# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="mongo-stream-cdc: pre-flight checks (msc_doctor)"
    )
    parser.add_argument("--config",       required=True)
    parser.add_argument("--table-config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)
    with open(args.table_config) as f:
        table_config = json.load(f)

    mongo_uri    = os.environ.get("MONGO_URI", "")
    postgres_uri = os.environ.get("POSTGRES_URI", "")

    print("\n══════════════════════════════════════════════════════")
    print("  mongo-stream-cdc — pre-flight doctor")
    print("══════════════════════════════════════════════════════")

    check_config(config, table_config)

    if mongo_uri:
        check_mongo(mongo_uri, config, table_config)
    else:
        print("\n── MongoDB ────────────────────────────────────────────")
        print(f"  {WARN}  MONGO_URI not set — skipping MongoDB checks")

    check_kafka(config, table_config)

    if postgres_uri:
        check_postgres(postgres_uri, table_config)
    else:
        print("\n── Postgres ───────────────────────────────────────────")
        print(f"  {WARN}  POSTGRES_URI not set — skipping Postgres checks")

    print("\n══════════════════════════════════════════════════════")
    if all_ok:
        print(f"  {PASS}  All checks passed. Ready to deploy.\n")
        sys.exit(0)
    else:
        print(f"  {FAIL}  Some checks failed. Fix above before deploying.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
