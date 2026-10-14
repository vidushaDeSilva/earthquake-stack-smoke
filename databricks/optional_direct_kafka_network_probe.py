# Databricks notebook source
"""Optional reachability check only; does not authenticate to or read Kafka."""

import socket

dbutils.widgets.text("aiven_host", "")
dbutils.widgets.text("aiven_port", "")
host = dbutils.widgets.get("aiven_host").strip()
port = int(dbutils.widgets.get("aiven_port"))
if not host or not (1 <= port <= 65535):
    raise ValueError("Provide the Aiven SASL host and port from Quick connect")

try:
    with socket.create_connection((host, port), timeout=6):
        print("PASS TCP reachability from Databricks serverless to Aiven")
        print("This does not yet verify Kafka authentication or the Spark connector.")
except OSError as exc:
    raise RuntimeError(
        "Databricks serverless cannot open a TCP connection to Aiven; "
        "the GitHub runner bridge is required for this workspace."
    ) from exc
