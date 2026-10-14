# Earthquake stack compatibility smoke test

This is a **manual, one-run cloud test**. It sends one real USGS earthquake
record through a dedicated Aiven Kafka topic, reads the same message on a
GitHub-hosted runner, uploads its bytes to a Databricks Volume, then runs a
Databricks serverless Spark notebook that reads the file and performs the same
Delta `MERGE` twice. It makes no claim of production reliability or continuous
earthquake coverage.

The laptop does not execute the workflow or store data. The test record passes
through runner memory; Aiven Kafka holds the message until its retention
period; Databricks stores the uploaded JSON and test table. GitHub stores the
code, encrypted credentials, and run logs (the latter contain IDs and status,
not earthquake payloads or secrets).

## What you need once

1. **Aiven:** Create a Free Apache Kafka service, wait for `Running`, then
   create a **Classic** topic named `earthquake_compat_smoke` with one partition.
   Open **Quick connect → Python → SASL/SCRAM-SHA-256**. Create/select a user
   with Produce and Consume permissions. Copy the SASL bootstrap host:port,
   username/password and download the CA certificate PEM. Do not commit these
   values. The free plan supports Kafka itself but not Kafka Connect.
2. **Databricks Free Edition:** In Catalog Explorer, create a managed Volume
   under a catalog/schema you can write to, such as
   `/Volumes/workspace/default/earthquake_compat`. Import
   `databricks/compatibility_notebook.py` as a Python notebook. Create a
   **serverless notebook Job** for it. Define three *job parameters* with
   these exact names: `probe_id`, `volume_root`, `target_table`. Their defaults
   may be `manual`, the test Volume path, and
   `workspace.default.earthquake_compat_probe`; the GitHub test overrides
   them. Copy the numeric Job ID.
3. **Databricks authentication:** Generate a workspace API token in your
   workspace's Developer settings. If token creation is disabled or the Files
   or Jobs API returns 401/403, that is a **failed compatibility gate** for
   this bridge design; check whether workspace OAuth automation is available
   before proceeding. Do not publish a token in repository code or output.
4. **GitHub:** Extract this ZIP and put the **contents** of
   `earthquake-stack-smoke/` at the repository root, so the workflow lives at
   `.github/workflows/compatibility.yml` on the default branch. Add these repository
   **variables** under Settings → Secrets and variables → Actions → Variables:

   | Name | Example |
   | --- | --- |
   | `AIVEN_BOOTSTRAP_SERVERS` | Aiven SASL host and port from Quick connect |
   | `AIVEN_TOPIC` | `earthquake_compat_smoke` |
   | `DATABRICKS_HOST` | `https://dbc-....cloud.databricks.com` |
   | `DATABRICKS_VOLUME_ROOT` | `/Volumes/workspace/default/earthquake_compat` |
   | `DATABRICKS_TARGET_TABLE` | `workspace.default.earthquake_compat_probe` |
   | `DATABRICKS_JOB_ID` | Numeric Job ID |

   Add these encrypted repository **secrets** under Actions → Secrets:

   | Name | Value |
   | --- | --- |
   | `AIVEN_KAFKA_USERNAME` | Quick connect username |
   | `AIVEN_KAFKA_PASSWORD` | Quick connect password |
   | `AIVEN_KAFKA_CA_PEM` | Full `-----BEGIN CERTIFICATE-----` PEM text |
   | `DATABRICKS_TOKEN` | Workspace API token |

## Run and interpret

In GitHub → Actions → **Earthquake stack compatibility** → **Run workflow**.
It is only `workflow_dispatch`; there is no schedule or ongoing cloud cost.
The normal output is:

```text
PASS USGS feed: event_id=... probe_id=...
PASS Kafka produce: topic=earthquake_compat_smoke partition=0 offset=...
PASS Kafka consume: partition=0 offset=...
PASS Databricks Files API: verified /Volumes/.../smoke/<probe_id>.json
Databricks Job started: run_id=...
PASS Databricks Spark + Delta MERGE: Job completed successfully
PASS end-to-end cloud compatibility smoke test
```

In Databricks, inspect the run ID under Jobs. The notebook reads the exact
probe file, runs `MERGE` twice, and asserts exactly one row for its `probe_id`.
Run the workflow a second time to test another record and confirm both Job
runs pass. Each run has a new `probe_id`, so the table should have two rows;
each individual probe still has exactly one. The test table and `smoke/` files
are isolated and can be deleted after reviewing the results.

| First failure | What it establishes |
| --- | --- |
| USGS GET | The hosted runner cannot reach the data source or it is unavailable. |
| Kafka producer | Aiven service, TLS, SASL credentials, ACL or runner network failed. |
| Kafka consumer | Produce succeeded but consume/ACL/topic access failed. |
| Volume upload/readback | Databricks authentication, Volume permissions or Files API failed. |
| Job trigger | Job ID, Jobs API permission or serverless Job setup failed. |
| Spark/Delta notebook | Volume read or Delta table permission/compute failed; inspect its run logs. |

## Optional direct Databricks → Kafka network test

Import `databricks/optional_direct_kafka_network_probe.py`, run it on the same
serverless compute and enter the Aiven **SASL** host and port from Quick
connect. This opens only a six-second TCP connection. A pass proves basic
network reachability, **not** Kafka authentication or Spark's Kafka connector.
A failure means the GitHub-runner bridge is needed for this workspace. Direct
Databricks Kafka ingestion remains an unverified alternative until a complete
authenticated Spark read is tested. Databricks Free Edition restricts outbound
internet access; some accounts can unlock additional access by verification.

## Guarantees this test does and does not establish

- It verifies one Kafka produce/consume cycle, a cloud-only upload with byte
  readback, a remote Job, Spark JSON parsing, and an idempotent Delta `MERGE`.
- Consumer offsets are committed only after a successful upload and readback.
  The test uses a unique consumer group per probe; it does **not** exercise a
  long-running group's recovery or production backpressure.
- Delta `MERGE` is atomic at the table level. Kafka, the Files API, and Delta
  do not share one cross-system transaction.
- This is not a retention, missing-run, schema-change, revision-history,
  deletion-reconciliation, or full load test. Those require separate gates
  before claiming the full pipeline works.
- Aiven Free has an idle shutdown and up to three days of Kafka retention.
  GitHub scheduled workflows can be delayed or dropped. A passing one-run
  test proves compatibility, not an unattended reliability guarantee.

## Primary references

- USGS GeoJSON feed: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
- Aiven Python Quick connect: https://aiven.io/docs/products/kafka/howto/connect-with-python
- Aiven Free tier: https://aiven.io/docs/products/kafka/free-tier/kafka-free-tier
- Databricks Volume Files API: https://docs.databricks.com/aws/en/volumes/volume-files
- Databricks Jobs run-now: https://docs.databricks.com/api/jobs/v2/run-now
- Databricks Free Edition limits: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
