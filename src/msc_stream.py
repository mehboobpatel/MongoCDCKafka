"""
msc_stream.py — mongo-stream-cdc
MongoDB Change Stream → Kafka

- Reads resume token from msc.resume Kafka topic (compacted, 1 partition)
- Falls back to oplog T0 from msc_snap on first run
- Publishes one Kafka topic per collection: msc.<collection>
- Leader election via K8s Lease — 2 pods run, only leader writes to Kafka
- SASL/PLAIN authentication (no SSL) for Kafka
"""

import argparse
import json
import logging
import os
import sys
import time
import certifi
from datetime import datetime, timezone

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException, TopicPartition
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError
from bson.timestamp import Timestamp
from bson.binary import Binary, UuidRepresentation
from bson.objectid import ObjectId
from msc_lease import KubernetesLease

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [msc_stream] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

RESUME_TOPIC = "msc.resume"


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


# ── Resume Point (Kafka-backed) ───────────────────────────────────

def load_resume_point_from_kafka(bootstrap_servers: str) -> dict | None:
    """
    Read the last message from msc.resume (compacted topic).
    Returns parsed resume point dict or None if topic is empty.
    """
    cfg = kafka_base_config()
    cfg.update({
        "bootstrap.servers":  bootstrap_servers,
        "group.id":           "msc-stream-resume-reader",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    consumer = Consumer(cfg)
    consumer.assign([TopicPartition(RESUME_TOPIC, 0, 0)])

    resume = None
    try:
        while True:
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())
            resume = json.loads(msg.value().decode("utf-8"))
    finally:
        consumer.close()

    if resume:
        logging.info("Loaded resume point from msc.resume topic")
    else:
        logging.info("msc.resume topic empty — will use oplog timestamp from snap")
    return resume


def save_resume_token_to_kafka(producer: Producer, token: dict):
    """
    Publish resume token to msc.resume (compacted topic).
    Key = 'resume' (fixed key — compaction retains only latest).
    """
    payload = {
        "resume_token": token,
        "updated_at":   datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(
        topic=RESUME_TOPIC,
        key=b"resume",
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.flush()


def _load_snap_timestamp() -> dict | None:
    """Read oplog T0 written by msc_snap from the msc-resume-point configmap via K8s API."""
    try:
        from kubernetes import client, config as k8s_config
        try:
            k8s_config.load_incluster_config()
        except Exception:
            logging.warning("Not running in a pod — cannot read configmap")
            return None

        v1        = client.CoreV1Api()
        namespace = os.environ.get("POD_NAMESPACE", "mongo-stream-cdc")
        cm        = v1.read_namespaced_config_map("msc-resume-point", namespace)
        data      = json.loads(cm.data.get("resume_point.json", "{}"))
        return data.get("timestamp")
    except Exception as e:
        logging.warning("Could not load snap timestamp from configmap: %s", e)
        return None


def build_watch_options(bootstrap_servers: str) -> dict:
    """
    Priority:
    1. resume_token from msc.resume  — stream was running, pick up exactly where it left off
    2. oplog T0 from snap configmap  — first start after msc_snap
    3. nothing                        — start from now (may miss events)
    """
    resume = load_resume_point_from_kafka(bootstrap_servers)

    if resume and "resume_token" in resume:
        logging.info("Resuming from Kafka-stored change stream token")
        return {
            "resume_after":  resume["resume_token"],
            "full_document": "updateLookup",
        }

    snap_ts = _load_snap_timestamp()
    if snap_ts:
        ts = Timestamp(snap_ts["t"], snap_ts["i"])
        logging.info("Starting from oplog timestamp: t=%s i=%s", snap_ts["t"], snap_ts["i"])
        return {
            "start_at_operation_time": ts,
            "full_document":           "updateLookup",
        }

    logging.warning("No resume point found — starting from now (events may be missed)")
    return {"full_document": "updateLookup"}


# ── Serialization ─────────────────────────────────────────────────

def serialize_doc(doc: dict) -> dict:
    if doc is None:
        return {}

    result = {}

    for k, v in doc.items():

        if isinstance(v, Binary):
            try:
                result[k] = str(v.as_uuid(UuidRepresentation.PYTHON_LEGACY))
            except Exception:
                result[k] = v.hex()

        elif isinstance(v, ObjectId):
            result[k] = str(v)

        elif hasattr(v, "as_doc"):
            result[k] = {"t": v.time, "i": v.inc}

        elif isinstance(v, datetime):
            result[k] = v.isoformat()

        elif isinstance(v, dict):
            result[k] = serialize_doc(v)

        elif isinstance(v, list):
            result[k] = [
                serialize_doc(i) if isinstance(i, dict)
                else str(i) if isinstance(i, ObjectId)
                else (
                    str(i.as_uuid(UuidRepresentation.PYTHON_LEGACY))
                    if isinstance(i, Binary)
                    else i
                )
                for i in v
            ]

        else:
            result[k] = v

    return result

def build_payload(change: dict, collection_name: str) -> dict:
    op         = change["operationType"]
    doc_id     = str(change["documentKey"]["_id"])
    full_doc   = serialize_doc(change.get("fullDocument") or {})
    cluster_ts = change.get("clusterTime")
    return {
        "collection":   collection_name,
        "operation":    op,
        "document_id":  doc_id,
        "document":     full_doc,
        "cluster_ts":   {"t": cluster_ts.time, "i": cluster_ts.inc} if cluster_ts else None,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Kafka Producer ────────────────────────────────────────────────

def make_producer(bootstrap_servers: list) -> Producer:
    cfg = kafka_base_config()
    cfg.update({
        "bootstrap.servers": ",".join(bootstrap_servers),
        "acks":               "all",
        "enable.idempotence": True,
        "retries":            10,
        "retry.backoff.ms":   500,
        "linger.ms":          10,
        "compression.type":   "lz4",
    })
    return Producer(cfg)


def delivery_report(err, msg):
    if err:
        logging.error("Kafka delivery failed: %s", err)
    else:
        logging.debug(
            "Delivered to %s [partition %d] offset %d",
            msg.topic(), msg.partition(), msg.offset()
        )


# ── Stream ────────────────────────────────────────────────────────

def topic_for(prefix: str, collection: str) -> str:
    return f"{prefix}.{collection.lower()}"


def watch_all_collections(
    mongo_db,
    producer: Producer,
    config: dict,
    table_names: list,
    lease: KubernetesLease,
):
    bootstrap_servers = ",".join(config["kafka_bootstrap_servers"])
    watch_options     = build_watch_options(bootstrap_servers)
    topic_prefix      = config.get("kafka_topic_prefix", "msc")

    pipeline = [{
        "$match": {
            "operationType": {"$in": ["insert", "update", "replace", "delete"]},
            "ns.coll":       {"$in": table_names},
        }
    }]

    logging.info("Watching %d collections from database: %s", len(table_names), mongo_db.name)

    while True:
        if not lease.is_leader():
            logging.error("Lost leadership — exiting stream loop")
            sys.exit(3)

        try:
            with mongo_db.watch(pipeline, **watch_options) as stream:
                logging.info("Change stream open. Listening...")
                for change in stream:

                    if not lease.is_leader():
                        logging.error("Lost leadership mid-stream — exiting")
                        sys.exit(3)

                    collection_name = change["ns"]["coll"]
                    doc_id  = str(change["documentKey"]["_id"])
                    payload = build_payload(change, collection_name)
                    topic   = topic_for(topic_prefix, collection_name)

                    producer.produce(
                        topic=topic,
                        key=doc_id.encode("utf-8"),
                        value=json.dumps(payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                    producer.flush()   # confirmed delivery before saving token

                    # Save resume token to Kafka AFTER confirmed delivery
                    save_resume_token_to_kafka(producer, stream.resume_token)

                    logging.info(
                        "[%s] %s doc_id=%s",
                        collection_name, payload["operation"], doc_id
                    )

                    watch_options = {
                        "resume_after":  stream.resume_token,
                        "full_document": "updateLookup",
                    }

        except OperationFailure as e:
            if "ChangeStreamHistoryLost" in str(e):
                logging.critical(
                    "Oplog history lost — resume token is too old. "
                    "Waiting for msc_snap to re-run and refresh the resume point. "
                    "Retrying in 5 minutes (not exiting, to avoid a restart storm)."
                )
                lease.release()
                time.sleep(300)
                watch_options = build_watch_options(bootstrap_servers)
                continue
            logging.error("MongoDB OperationFailure: %s — retrying in 10s", e)
            time.sleep(10)

        except PyMongoError as e:
            logging.error("MongoDB error: %s — retrying in 5s", e)
            time.sleep(5)
            watch_options = build_watch_options(bootstrap_servers)

        except Exception as e:
            logging.error("Unexpected error: %s — retrying in 5s", e)
            time.sleep(5)


# ── Main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="mongo-stream-cdc: change stream producer (msc_stream)"
    )
    parser.add_argument("--config",       required=True)
    parser.add_argument("--table-config", required=True)
    args = parser.parse_args()

    config       = load_config(args.config)
    table_config = load_table_config(args.table_config)
    table_names  = [t["table_name"] for t in table_config]

    mongo_uri         = os.environ["MONGO_URI"]
    namespace         = os.environ.get("POD_NAMESPACE", "mongo-stream-cdc")
    bootstrap_servers = ",".join(config["kafka_bootstrap_servers"])

    # Wait for snap timestamp to exist in configmap
    for attempt in range(30):
        if _load_snap_timestamp():
            break
        logging.info("Waiting for snap timestamp in configmap (%d/30)...", attempt + 1)
        time.sleep(10)
    else:
        logging.error("Snap timestamp not found after 5 min. Run msc_snap first.")
        sys.exit(1)

    mongo_client = MongoClient(mongo_uri)
    mongo_db     = mongo_client[config["mongo_database"]]
    producer     = make_producer(config["kafka_bootstrap_servers"])

    lease = KubernetesLease(name="msc-stream-leader", namespace=namespace)

    try:
        with lease:
            watch_all_collections(mongo_db, producer, config, table_names, lease)
    finally:
        producer.flush()
        mongo_client.close()


if __name__ == "__main__":
    main()
