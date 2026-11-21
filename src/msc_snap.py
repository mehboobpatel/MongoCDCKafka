"""
msc_snap.py — mongo-stream-cdc
One-time snapshot: MongoDB → Postgres

Steps:
  1. Capture oplog timestamp T0 (BEFORE any reads)
  2. For each collection in table_config: export → load into Postgres
  3. Create Postgres indexes mirrored from MongoDB index definitions
  4. Save T0 to msc-resume-point ConfigMap for msc_stream to pick up

Write strategy:
  > copy_threshold docs → COPY via tab-delimited text buffer (5-10x faster for large collections)
  ≤ copy_threshold docs → execute_batch (simpler, sufficient for small collections)

COPY format note:
  Postgres COPY text format uses tab delimiters + backslash escaping.
  It does NOT use CSV-style quoting. Using csv.writer caused double-quote
  escaping ("") that Postgres rejected for JSONB columns. Fixed to write
  raw tab-separated lines with backslash escapes only.
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
# Note: csv module intentionally NOT imported — Postgres COPY text format uses
# backslash escaping, not CSV quoting. Using csv.writer caused JSONB corruption.

import psycopg2
from psycopg2.extras import execute_batch
from pymongo import MongoClient
from bson import ObjectId, Decimal128
from bson.timestamp import Timestamp as BsonTimestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [msc_snap] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

COPY_THRESHOLD = 100_000

# MongoDB index types that have no meaningful flat-schema Postgres equivalent
_SKIP_INDEX_TYPES = {"text", "2dsphere", "2d", "hashed", "geoHaystack"}


# ── Config ────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)

def load_table_config(path: str) -> list:
    with open(path) as f:
        data = json.load(f)
    # Auto-upgrade varchar(N) → TEXT to prevent truncation errors.
    # MongoDB has no length limits so varchar caps will always risk failing.
    for table in data:
        table["schema"] = [
            re.sub(r'(?i)varchar\(\d+\)', 'TEXT', field)
            for field in table.get("schema", [])
        ]
    return data


# ── Oplog Timestamp ───────────────────────────────────────────────

def capture_oplog_timestamp(mongo_client: MongoClient) -> dict:
    oplog  = mongo_client["local"]["oplog.rs"]
    latest = oplog.find_one(sort=[("$natural", -1)])
    if not latest:
        raise RuntimeError("Could not read oplog — is MongoDB running as a replica set?")
    ts = latest["ts"]
    logging.info("Captured oplog timestamp: t=%s i=%s", ts.time, ts.inc)
    return {"t": ts.time, "i": ts.inc}


def save_resume_point_to_configmap(ts: dict, namespace: str):
    from kubernetes import client, config as k8s_config

    # FIX: Do not fall back to load_kube_config() inside a pod.
    # Previously the bare `except Exception: k8s_config.load_kube_config()`
    # silently fell through when load_incluster_config() failed, producing an
    # unauthenticated client (system:anonymous) and a 403 Forbidden error.
    # Now we raise immediately so the real cause is visible in the logs.
    try:
        k8s_config.load_incluster_config()
    except Exception as e:
        raise RuntimeError(
            f"Failed to load in-cluster K8s config — "
            f"is the pod running with a service account token? {e}"
        )

    v1      = client.CoreV1Api()
    payload = json.dumps({
        "timestamp":   ts,
        "captured_at": datetime.now(timezone.utc).isoformat(),
    })

    body = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(name="msc-resume-point", namespace=namespace),
        data={"resume_point.json": payload},
    )

    try:
        v1.read_namespaced_config_map("msc-resume-point", namespace)
        v1.replace_namespaced_config_map("msc-resume-point", namespace, body)
        logging.info("Updated msc-resume-point ConfigMap with T0")
    except Exception:
        v1.create_namespaced_config_map(namespace, body)
        logging.info("Created msc-resume-point ConfigMap with T0")


# ── Schema Helpers ────────────────────────────────────────────────

def extract_column_names(schema_fields: list) -> list:
    columns = []
    for field in schema_fields:
        field = field.strip()
        if field.upper().startswith("PRIMARY KEY"):
            continue
        columns.append(field.split()[0])
    return columns

def build_create_table_sql(table_name: str, schema_fields: list) -> str:
    schema_str = ",\n  ".join(schema_fields)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {schema_str}\n)"

def build_upsert_sql(table_name: str, columns: list) -> str:
    placeholders = ", ".join(["%s"] * len(columns))
    col_list     = ", ".join(columns)
    update_cols  = [c for c in columns if c != "id"]
    set_clause   = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    return (
        f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {set_clause}"
    )

def build_schema_map(schema_fields: list) -> dict:
    schema_map = {}
    for field in schema_fields:
        field = field.strip()
        if field.upper().startswith("PRIMARY KEY"):
            continue
        parts = field.split(None, 1)
        if len(parts) == 2:
            schema_map[parts[0]] = parts[1]
    return schema_map


# ── Index Mirroring ───────────────────────────────────────────────

def create_indexes_from_mongo(mongo_db, pg_conn, collection_name: str, table_name: str, columns: set):
    """
    Read MongoDB index definitions and create equivalent Postgres indexes.

    Mapping rules:
      - Single / compound field indexes  → CREATE INDEX
      - unique: true                     → CREATE UNIQUE INDEX
      - sparse: true (single field)      → WHERE col IS NOT NULL
      - Nested fields (a.b)              → skipped (flat schema)
      - text / 2dsphere / hashed / 2d   → skipped (no pg equivalent)
      - _id index                        → skipped (covered by PRIMARY KEY)
    """
    try:
        index_info = mongo_db[collection_name].index_information()
    except Exception as e:
        logging.warning("%s: could not read mongo indexes — %s", collection_name, e)
        return

    for idx_name, idx in index_info.items():
        if idx_name == "_id_":
            continue

        keys = idx.get("key", [])

        # Skip indexes that contain unsupported types
        if any(str(direction) in _SKIP_INDEX_TYPES for _, direction in keys):
            logging.debug("%s: skipping unsupported index type — %s", table_name, idx_name)
            continue

        # Resolve column names — skip nested fields and fields not in schema
        pg_cols = []
        for field, _ in keys:
            if "." in field:
                continue
            if field in columns:
                pg_cols.append(field)

        if not pg_cols:
            logging.debug("%s: skipping index %s — no matching columns", table_name, idx_name)
            continue

        unique       = idx.get("unique", False)
        sparse       = idx.get("sparse", False)
        col_str      = "_".join(pg_cols)
        pg_idx_name  = f"idx_{table_name}_{col_str}"[:63]  # Postgres 63-char name limit
        col_list     = ", ".join(pg_cols)
        unique_kw    = "UNIQUE " if unique else ""

        # Sparse single-field → partial index
        where_clause = ""
        if sparse and len(pg_cols) == 1:
            where_clause = f" WHERE {pg_cols[0]} IS NOT NULL"

        sql = (
            f"CREATE {unique_kw}INDEX IF NOT EXISTS {pg_idx_name} "
            f"ON {table_name} ({col_list}){where_clause}"
        )

        try:
            with pg_conn.cursor() as cur:
                cur.execute(sql)
            pg_conn.commit()
            logging.info("%s: index created — %s (%s)%s",
                         table_name, pg_idx_name, col_list,
                         " UNIQUE" if unique else "")
        except Exception as e:
            pg_conn.rollback()
            logging.warning("%s: skipped index %s — %s", table_name, pg_idx_name, e)


# ── Universal Type Coercion ───────────────────────────────────────

def _deep_strip_nullbytes(obj):
    """Recursively strip \\u0000 from all strings in a nested structure."""
    if isinstance(obj, str):
        return obj.replace("\u0000", "")
    if isinstance(obj, dict):
        return {k: _deep_strip_nullbytes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_strip_nullbytes(i) for i in obj]
    return obj


def _json_default(obj):
    """JSON serializer for all BSON/Python types json.dumps cannot handle."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    if isinstance(obj, (bytes, bytearray)):
        return bytes(obj).hex()
    if isinstance(obj, BsonTimestamp):
        return {"t": obj.time, "i": obj.inc}
    try:
        return str(obj)
    except Exception:
        return None


def coerce_value(value, col_name: str, schema_map: dict):
    if value is None:
        return None

    col_type = schema_map.get(col_name, "").lower()

    # ── JSONB / JSON ──────────────────────────────────────────────
    if "jsonb" in col_type or "json" in col_type:
        if not isinstance(value, (dict, list)):
            value = {"v": value}
        # Deep strip null bytes before serializing — Postgres rejects \u0000 in jsonb
        return json.dumps(_deep_strip_nullbytes(value), default=_json_default)

    # ── UUID ──────────────────────────────────────────────────────
    if "uuid" in col_type:
        if isinstance(value, (bytes, bytearray)):
            try:
                return str(uuid.UUID(bytes=bytes(value)))
            except Exception:
                return None
        try:
            uuid.UUID(str(value))
            return str(value)
        except (ValueError, AttributeError):
            return None  # silently drop non-UUID strings (e.g. "Haptik")

    # ── Boolean ───────────────────────────────────────────────────
    if "bool" in col_type:
        if isinstance(value, bool):
            return value
        return str(value).lower() in ("true", "1", "yes")

    # ── Integer ───────────────────────────────────────────────────
    if any(t in col_type for t in ("integer", "int", "bigint", "smallint")):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # ── Float / Numeric ───────────────────────────────────────────
    if any(t in col_type for t in ("float", "numeric", "decimal", "real", "double")):
        if isinstance(value, Decimal128):
            return float(value.to_decimal())
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    # ── Timestamp ─────────────────────────────────────────────────
    if "timestamp" in col_type:
        if not value or value == "":
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, BsonTimestamp):
            return datetime.fromtimestamp(value.time, tz=timezone.utc)
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    # ── BSON fallbacks for untyped / text columns ─────────────────
    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, Decimal128):
        return float(value.to_decimal())

    if isinstance(value, BsonTimestamp):
        return datetime.fromtimestamp(value.time, tz=timezone.utc).isoformat()

    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8").replace("\u0000", "")
        except UnicodeDecodeError:
            return bytes(value).hex()

    # ── Default: stringify and strip null bytes ───────────────────
    result = str(value) if not isinstance(value, str) else value
    return result.replace("\u0000", "")


# ── Write Strategies ──────────────────────────────────────────────

def _flush_batch_execute(pg_conn, sql: str, batch: list):
    with pg_conn.cursor() as cur:
        execute_batch(cur, sql, batch)
    pg_conn.commit()


def _safe_copy_value(v) -> str:
    """
    Serialize a single value for Postgres COPY text format.

    Postgres COPY text format is tab-delimited with backslash escaping.
    It does NOT use CSV-style quoting — using csv.writer was the root cause
    of the JSONB corruption bug: csv.writer wraps strings containing double
    quotes in outer quotes and doubles the inner quotes (""), which Postgres
    COPY rejects with "invalid input syntax for type json".

    Rules:
      - None         → literal \\N  (Postgres NULL sentinel)
      - bool         → 't' / 'f'   (Postgres boolean text representation)
      - bytes        → decode UTF-8 or hex-encode
      - str/other    → backslash-escape: \\ \\n \\r \\t and strip \\u0000
    No outer quoting is added — the tab delimiter is sufficient.
    """
    if v is None:
        return "\\N"
    if isinstance(v, bool):
        # Must check bool before int — bool is a subclass of int in Python
        return "t" if v else "f"
    if isinstance(v, (bytes, bytearray)):
        try:
            v = bytes(v).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(v).hex()
    s = str(v)
    # Escape backslash first, then special chars
    s = s.replace("\\", "\\\\")
    s = s.replace("\n", "\\n")
    s = s.replace("\r", "\\r")
    s = s.replace("\t", "\\t")
    s = s.replace("\u0000", "")
    return s


def _flush_batch_copy(pg_conn, staging: str, columns: list, batch: list):
    """
    Write rows to Postgres using COPY text format (tab-delimited, backslash-escaped).
    No csv.writer — we build the buffer manually to avoid CSV quoting mangling JSON.
    """
    buf = io.StringIO()
    for row in batch:
        buf.write("\t".join(_safe_copy_value(v) for v in row))
        buf.write("\n")
    buf.seek(0)
    col_list = ", ".join(columns)
    with pg_conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {staging} ({col_list}) FROM STDIN WITH (FORMAT text, NULL '\\N')",
            buf,
        )
    pg_conn.commit()


# ── Table Snapshot ────────────────────────────────────────────────

def snapshot_collection(mongo_db, pg_conn, table_config: dict, batch_size: int, copy_threshold: int):
    table_name    = table_config["table_name"]
    metabase_name = table_config.get("metabase_table_name", table_name)
    schema_fields = table_config["schema"]
    query_str     = table_config.get("query", "")
    columns       = extract_column_names(schema_fields)
    schema_map    = build_schema_map(schema_fields)

    logging.info("=== Snapshotting: %s → %s ===", table_name, metabase_name)

    mongo_filter = {}
    if query_str:
        try:
            mongo_filter = json.loads(query_str)
        except Exception as e:
            logging.warning("%s: invalid query JSON, ignoring filter (%s)", table_name, e)
            mongo_filter = {}

    logging.info("%s: filter = %s", table_name, mongo_filter)

    collection      = mongo_db[table_name]
    total_no_filter = collection.count_documents({})
    total_filtered  = collection.count_documents(mongo_filter)

    logging.info("%s: %d docs (filtered) / %d docs (total)", table_name, total_filtered, total_no_filter)

    if total_filtered == 0 and total_no_filter > 0:
        logging.warning("%s: filter returned 0 but collection has data — falling back to full scan", table_name)
        mongo_filter = {}
        total = total_no_filter
    else:
        total = total_filtered

    if total == 0:
        logging.info("%s: empty collection, skipping", table_name)
        _ensure_table(pg_conn, metabase_name, schema_fields)
        return

    use_copy   = total > copy_threshold
    logging.info("%s: using %s", table_name, "COPY" if use_copy else "execute_batch")

    staging    = f"{metabase_name}_new"
    _drop_and_create_staging(pg_conn, staging, schema_fields)
    upsert_sql = build_upsert_sql(staging, columns) if not use_copy else None

    batch    = []
    imported = 0
    skipped  = 0
    projection = {col: 1 for col in columns}
    projection["_id"] = 0

    for doc in collection.find(mongo_filter, projection, batch_size=batch_size):
        row = tuple(coerce_value(doc.get(col), col, schema_map) for col in columns)

        # Skip rows where id is null — violates PK not-null constraint
        if "id" in columns and row[columns.index("id")] is None:
            logging.warning("%s: skipping doc with null id", table_name)
            skipped += 1
            continue

        batch.append(row)

        if len(batch) >= batch_size:
            if use_copy:
                _flush_batch_copy(pg_conn, staging, columns, batch)
            else:
                _flush_batch_execute(pg_conn, upsert_sql, batch)
            imported += len(batch)
            batch = []
            if imported % 10000 == 0:
                logging.info("%s: %d / %d imported", table_name, imported, total)

    if batch:
        if use_copy:
            _flush_batch_copy(pg_conn, staging, columns, batch)
        else:
            _flush_batch_execute(pg_conn, upsert_sql, batch)
        imported += len(batch)

    if skipped:
        logging.warning("%s: skipped %d rows with null id", table_name, skipped)

    _atomic_swap(pg_conn, metabase_name, staging)
    logging.info("%s: done — %d rows imported", metabase_name, imported)

    # Mirror MongoDB indexes to Postgres after the swap
    create_indexes_from_mongo(mongo_db, pg_conn, table_name, metabase_name, set(columns))


def _ensure_table(pg_conn, table_name: str, schema_fields: list):
    with pg_conn.cursor() as cur:
        cur.execute(build_create_table_sql(table_name, schema_fields))
    pg_conn.commit()

def _drop_and_create_staging(pg_conn, staging: str, schema_fields: list):
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        cur.execute(build_create_table_sql(staging, schema_fields))
    pg_conn.commit()
    logging.info("Created staging table: %s", staging)

def _atomic_swap(pg_conn, target: str, staging: str):
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {target}")
        cur.execute(f"ALTER TABLE {staging} RENAME TO {target}")
    pg_conn.commit()
    logging.info("Swapped %s → %s", staging, target)


# ── Per-collection runner (own Postgres connection) ───────────────

def snapshot_one(mongo_db, postgres_uri: str, tc: dict, batch_size: int, copy_threshold: int) -> str | None:
    """Snapshot a single collection in its own Postgres connection.
    Returns table_name on failure, None on success."""
    pg_conn = psycopg2.connect(postgres_uri)
    try:
        snapshot_collection(mongo_db, pg_conn, tc, batch_size, copy_threshold)
        return None
    except Exception as e:
        logging.error("FAILED: %s — %s", tc["table_name"], e)
        try:
            pg_conn.rollback()
        except Exception:
            pass
        return tc["table_name"]
    finally:
        pg_conn.close()


# ── Main ──────────────────────────────────────────────────────────

def main():
    global COPY_THRESHOLD

    parser = argparse.ArgumentParser(description="mongo-stream-cdc: one-time snapshot (msc_snap)")
    parser.add_argument("--config",         required=True)
    parser.add_argument("--table-config",   required=True)
    parser.add_argument("--tables",         nargs="*")
    parser.add_argument("--batch-size",     type=int, default=500)
    parser.add_argument("--copy-threshold", type=int, default=COPY_THRESHOLD)
    parser.add_argument("--parallelism",    type=int, default=1,
                        help="Collections to snapshot in parallel (default: 1 = sequential)")
    args = parser.parse_args()

    copy_threshold = args.copy_threshold
    config         = load_config(args.config)
    table_config   = load_table_config(args.table_config)
    namespace      = os.environ.get("POD_NAMESPACE", "mongo-stream-cdc")
    mongo_uri      = os.environ["MONGO_URI"]
    postgres_uri   = os.environ["POSTGRES_URI"]

    if args.tables:
        table_config = [
            t for t in table_config
            if t["table_name"] in args.tables
            or t.get("metabase_table_name", "") in args.tables
        ]
        logging.info("Snapshotting specified tables: %s", args.tables)
    else:
        logging.info("Snapshotting all %d tables", len(table_config))

    mongo_client = MongoClient(mongo_uri)
    mongo_db     = mongo_client[config["mongo_database"]]

    # Step 1: Capture oplog timestamp BEFORE any reads
    ts = capture_oplog_timestamp(mongo_client)
    save_resume_point_to_configmap(ts, namespace)

    # Step 2: Snapshot each collection (each gets its own Postgres connection)
    failed = []
    total  = len(table_config)

    if args.parallelism <= 1:
        for i, tc in enumerate(table_config, 1):
            logging.info("── [%d/%d] %s", i, total, tc["table_name"])
            err = snapshot_one(mongo_db, postgres_uri, tc, args.batch_size, copy_threshold)
            if err:
                failed.append(err)
    else:
        logging.info("Running with parallelism=%d", args.parallelism)
        with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
            futures = {
                pool.submit(snapshot_one, mongo_db, postgres_uri, tc, args.batch_size, copy_threshold): tc
                for tc in table_config
            }
            done = 0
            for future in as_completed(futures):
                done += 1
                tc  = futures[future]
                err = future.result()
                logging.info(
                    "── [%d/%d] %s — %s",
                    done, total, tc["table_name"], "OK" if not err else "FAILED",
                )
                if err:
                    failed.append(err)

    mongo_client.close()

    if failed:
        logging.error("Snapshot completed with %d error(s): %s", len(failed), failed)
        sys.exit(1)
    else:
        logging.info("Snapshot complete — all %d tables imported. Start msc_stream and msc_sink.", total)


if __name__ == "__main__":
    main()

