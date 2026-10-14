"""One cloud-runner smoke test: USGS -> Kafka -> Volume -> Databricks Job.

No earthquake payload or credential is written to the GitHub runner disk.
This uses a dedicated Kafka topic and a disposable Databricks Volume prefix.
"""

import hashlib
import json
import os
import re
import ssl
import time
import uuid
from urllib.parse import quote

import requests
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata

USGS_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


def required(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing configuration: {name}")
    return value


def kafka_config():
    # Aiven Quick connect supplies the SASL endpoint and downloadable CA PEM.
    context = ssl.create_default_context(cadata=required("AIVEN_KAFKA_CA_PEM"))
    return dict(
        bootstrap_servers=required("AIVEN_BOOTSTRAP_SERVERS"),
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=required("AIVEN_KAFKA_USERNAME"),
        sasl_plain_password=required("AIVEN_KAFKA_PASSWORD"),
        ssl_context=context,
        request_timeout_ms=20000,
        api_version_auto_timeout_ms=10000,
    )


def databricks_session():
    host = required("DATABRICKS_HOST").rstrip("/")
    if not host.startswith("https://"):
        raise ValueError("DATABRICKS_HOST must start with https://")
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {required('DATABRICKS_TOKEN')}"})
    return session, host


def fetch_one_event():
    response = requests.get(USGS_FEED, timeout=25)
    response.raise_for_status()
    events = response.json().get("features", [])
    if not events:
        raise RuntimeError("USGS past-day feed has no events; retry the probe later")
    event = max(events, key=lambda item: item["properties"]["time"])
    if not event.get("id") or event["properties"].get("updated") is None:
        raise RuntimeError("USGS event lacks id or updated timestamp")
    return event


def upload_and_verify(session, host, volume_root, probe_id, payload):
    if not re.fullmatch(r"/Volumes/[^/]+/[^/]+/[^/]+", volume_root):
        raise ValueError("DATABRICKS_VOLUME_ROOT must be /Volumes/catalog/schema/volume")
    path = f"{volume_root}/smoke/{probe_id}.json"
    directory = f"{host}/api/2.0/fs/directories{quote(volume_root + '/smoke', safe='/')}"
    created = session.put(directory, timeout=30)
    # The smoke directory may already exist from a previous run.
    if created.status_code not in (200, 201, 204, 409):
        created.raise_for_status()
    uri = f"{host}/api/2.0/fs/files{quote(path, safe='/')}"
    headers = {"Content-Type": "application/octet-stream"}
    response = session.put(uri, params={"overwrite": "false"}, data=payload,
                           headers=headers, timeout=30)
    response.raise_for_status()
    # Reading back the small payload verifies the actual bytes, not just an HTTP ACK.
    verify = session.get(uri, timeout=30)
    verify.raise_for_status()
    if hashlib.sha256(verify.content).digest() != hashlib.sha256(payload).digest():
        raise RuntimeError("Databricks Volume readback checksum mismatch")
    return path


def publish_then_land(record, probe_id):
    topic = required("AIVEN_TOPIC")
    if topic != "earthquake_compat_smoke":
        raise ValueError("Use a dedicated topic named earthquake_compat_smoke")
    config = kafka_config()
    payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
    producer = KafkaProducer(**config, acks="all", value_serializer=lambda value: value)
    try:
        metadata = producer.send(topic, key=probe_id.encode("ascii"), value=payload).get(timeout=30)
        print(f"PASS Kafka produce: topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}")
    finally:
        producer.close(timeout=10)

    # Unique consumer group starts at earliest so it can find this one probe
    # even when older smoke messages remain in the topic.
    consumer = KafkaConsumer(
        topic, **config, group_id=f"eq-compat-{probe_id}",
        auto_offset_reset="earliest", enable_auto_commit=False,
    )
    session, host = databricks_session()
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline:
            for partition_messages in consumer.poll(timeout_ms=2000, max_records=100).values():
                for message in partition_messages:
                    if message.key != probe_id.encode("ascii"):
                        continue
                    if message.value != payload:
                        raise RuntimeError("Kafka round-trip payload changed")
                    print(f"PASS Kafka consume: partition={message.partition} offset={message.offset}")
                    path = upload_and_verify(session, host,
                                             required("DATABRICKS_VOLUME_ROOT"),
                                             probe_id, message.value)
                    # Commit this exact message only after the cloud upload is verified.
                    # kafka-python 2.3+ includes leader_epoch; earlier releases do not.
                    try:
                        committed_offset = OffsetAndMetadata(message.offset + 1, "", -1)
                    except TypeError:
                        committed_offset = OffsetAndMetadata(message.offset + 1, "")
                    consumer.commit({TopicPartition(message.topic, message.partition): committed_offset})
                    print(f"PASS Databricks Files API: verified {path}")
                    return session, host
        raise TimeoutError("Kafka consumer did not see this probe in 90 seconds")
    finally:
        consumer.close()


def run_databricks_job(session, host, probe_id):
    table = required("DATABRICKS_TARGET_TABLE")
    if not re.fullmatch(r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*\.[A-Za-z_][\w]*", table):
        raise ValueError("DATABRICKS_TARGET_TABLE must be catalog.schema.table")
    job_id = int(required("DATABRICKS_JOB_ID"))
    params = {
        "probe_id": probe_id,
        "volume_root": required("DATABRICKS_VOLUME_ROOT"),
        "target_table": table,
    }
    response = session.post(
        f"{host}/api/2.2/jobs/run-now",
        json={"job_id": job_id, "job_parameters": params,
              "idempotency_token": probe_id}, timeout=30,
    )
    response.raise_for_status()
    run_id = response.json()["run_id"]
    print(f"Databricks Job started: run_id={run_id}")
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        run = session.get(f"{host}/api/2.2/jobs/runs/get",
                          params={"run_id": run_id}, timeout=30)
        run.raise_for_status()
        state = run.json()["state"]
        if state.get("result_state") == "SUCCESS":
            print("PASS Databricks Spark + Delta MERGE: Job completed successfully")
            return
        if state.get("life_cycle_state") in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            raise RuntimeError(f"Databricks Job failed, run_id={run_id}, "
                               f"state={state.get('result_state')}; inspect Job run logs")
        time.sleep(10)
    raise TimeoutError(f"Databricks Job still running: run_id={run_id}")


def main():
    probe_id = str(uuid.uuid4())
    event = fetch_one_event()
    print(f"PASS USGS feed: event_id={event['id']} probe_id={probe_id}")
    record = {"probe_id": probe_id, "earthquake": event}
    session, host = publish_then_land(record, probe_id)
    run_databricks_job(session, host, probe_id)
    print("PASS end-to-end cloud compatibility smoke test")


if __name__ == "__main__":
    main()
