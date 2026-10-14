# Databricks notebook source
"""Serverless notebook task: verify Kafka-landed JSON and idempotent Delta MERGE."""

import re
from pyspark.sql import functions as F

dbutils.widgets.text("probe_id", "")
dbutils.widgets.text("volume_root", "/Volumes/catalog/schema/volume")
dbutils.widgets.text("target_table", "catalog.schema.compat_probe")

probe_id = dbutils.widgets.get("probe_id")
volume_root = dbutils.widgets.get("volume_root").rstrip("/")
target_table = dbutils.widgets.get("target_table")

if not re.fullmatch(r"[0-9a-f-]{36}", probe_id):
    raise ValueError("Invalid probe_id")
if not re.fullmatch(r"/Volumes/[^/]+/[^/]+/[^/]+", volume_root):
    raise ValueError("Invalid volume_root")
if not re.fullmatch(r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*\.[A-Za-z_][\w]*", target_table):
    raise ValueError("Invalid target_table")

qualified_table = ".".join(f"`{part}`" for part in target_table.split("."))
path = f"{volume_root}/smoke/{probe_id}.json"
input_df = spark.read.json(path)
rows = input_df.where(F.col("probe_id") == probe_id)
assert rows.count() == 1, "Exactly one matching Kafka message is required"

source = rows.select(
    "probe_id",
    F.col("earthquake.id").alias("event_id"),
    F.col("earthquake.properties.updated").cast("long").alias("event_updated_ms"),
    F.sha2(F.to_json("earthquake"), 256).alias("payload_sha256"),
)
assert source.where("event_id IS NULL OR event_updated_ms IS NULL").count() == 0
source.createOrReplaceTempView("compat_probe_source")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {qualified_table} (
  probe_id STRING, event_id STRING, event_updated_ms BIGINT,
  payload_sha256 STRING, received_at TIMESTAMP
) USING DELTA
""")

merge_sql = f"""
MERGE INTO {qualified_table} AS t
USING compat_probe_source AS s ON t.probe_id = s.probe_id
WHEN MATCHED THEN UPDATE SET
  t.event_id = s.event_id,
  t.event_updated_ms = s.event_updated_ms,
  t.payload_sha256 = s.payload_sha256
WHEN NOT MATCHED THEN INSERT (
  probe_id, event_id, event_updated_ms, payload_sha256, received_at
) VALUES (s.probe_id, s.event_id, s.event_updated_ms, s.payload_sha256, current_timestamp())
"""
spark.sql(merge_sql)
spark.sql(merge_sql)  # A retry must not create a second row.
result = spark.table(target_table).where(F.col("probe_id") == probe_id)
assert result.count() == 1, "Delta MERGE was not idempotent"
print(f"PASS Spark JSON read + two atomic Delta MERGEs: {probe_id}")
display(result)
