# Part 1: streaming telematics ingestion with Kafka (design)

This design follows part 1 of [transcript_1.txt](transcript_1.txt), which covers medallion schemas and streaming telematics into bronze. Two things change: **Confluent Cloud Kafka** replaces Amazon Kinesis, and a simulator we build replaces the author's hidden script.

## Constraints

- **No new catalogs.** Use the existing `e2e_dev` / `e2e_prod` catalogs and add only schemas.
- **Placeholder credentials.** No real Confluent account exists yet, so every connection value is a placeholder (`<BOOTSTRAP_SERVER>`, `<API_KEY>`, `<API_SECRET>`). The config must be correct, so that real values are the only thing missing.
- **Network risk.** Free Edition may limit outbound internet access from serverless compute. We can only confirm the pipeline reaches Confluent once real credentials exist, which is step 11 of the plan.
- **No git commits** while following along.

## Architecture

```text
simulator (laptop: `uv run simulate`, or the bundle job `telematics_simulator`)
      │  JSON telematics events, key = chassis_number
      ▼
Confluent Cloud topic `telematics`            (SASL_SSL + PLAIN, API key/secret)
      │
      ▼
Lakeflow pipeline `telematics_ingestion`       (serverless, triggered, default schema: bronze)
  ├─ telematics_raw  – streaming table: raw key/value bytes + Kafka metadata
  └─ telematics      – streaming table: parsed columns, read from telematics_raw
```

### Schemas

The bundle owns four new schema resources in `${var.catalog}`: `landing`, `bronze`, `silver` and `gold`.

- **dev:** development mode names them `dev_<user>_landing` and so on.
- **prod:** they're named `landing`, `bronze`, `silver` and `gold`.
- **Part 1** writes only to `bronze`. The other three are there for later parts.

The existing sample schema and the sample job and pipeline don't change.

### Credentials and configuration

- **Secret scope:** the bundle defines a secret scope, `kafka`, with keys `bootstrap_servers`, `api_key` and `api_secret`. Values are set only with `databricks secrets put-secret`, never in YAML or git.
- **Code references:** code finds the scope by name through `${resources.secret_scopes.kafka.name}`, not a hardcoded name.
- **Topic:** the bundle variable `telematics_topic` sets the topic, default `telematics`.
- **One source for credentials:** the pipeline, the simulator job and the local simulator all read the same scope. The local simulator uses `databricks.sdk.runtime.dbutils.secrets`, which works through the CLI profile.

## Units

All shared logic lives in `src/databricks_end_to_end_project/telematics/`, so tests can cover it and both the pipeline and the simulator can reuse it.

| Unit | Responsibility | Depends on |
| --- | --- | --- |
| `events.py` | `generate_event(rng, chassis_numbers, now)` returns one fake event dict: `chassis_number`, `speed`, `latitude`, `longitude`, `event_timestamp`. `to_json(event)` returns the event as bytes. | stdlib |
| `kafka_config.py` | `spark_kafka_options(servers, key, secret, topic)` returns the Spark `readStream` options for Confluent: `SASL_SSL`, `PLAIN`, and the `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule` JAAS line, with `startingOffsets=earliest`. `producer_config(servers, key, secret)` returns the settings for the `confluent-kafka` producer. | stdlib |
| `parsing.py` | `parse_telematics(raw_df)` casts `value` to the string `raw_json`, parses it with `from_json` as `map<string,string>`, and outputs the event columns as strings, as in the transcript. It also adds a `stream_metadata` struct with the topic, partition, offset and timestamp. | PySpark |
| `simulator.py` | `run(send, topic, count, interval, rng)` is the loop, and `send` is injected. `main()` is the `simulate` console script, with `--count`, `--interval`, `--topic`, `--secret-scope` and `--dry-run`. With `--dry-run`, it prints events and needs no Kafka and no secrets. | the three units above, plus `confluent-kafka` |

Pipeline sources are in `src/telematics_ingestion/transformations/`, one dataset per file:

- **`telematics_raw.py`:** `@dp.table`, using `spark.readStream.format("kafka").options(**spark_kafka_options(...))`. It reads the scope and topic from the pipeline `configuration`.
- **`telematics.py`:** `@dp.table`, returning `parse_telematics(spark.readStream.table("telematics_raw"))`.

Bundle resources:

- **`resources/databricks_end_to_end_project.schema.yml`:** the 4 schemas, added next to the existing project schema
- **`resources/kafka.secret_scope.yml`:** the secret scope
- **`resources/telematics_ingestion.pipeline.yml`:** serverless and triggered. Its schema is `${resources.schemas.bronze.name}`, and its `configuration` holds the topic and the secret scope name.
- **`resources/telematics_simulator.job.yml`:** a wheel task running `simulate --count 50 --interval 3`, with no schedule
- **`databricks.yml`:** adds the variable `telematics_topic`
- **`pyproject.toml`:** adds the dependency `confluent-kafka` and the script `simulate = databricks_end_to_end_project.telematics.simulator:main`

## Kafka bypass: landing files (added 2026-09-28)

With no Confluent account, the Kafka path can be deployed, but no data can flow through it. So events can also travel as files:

```text
simulate --sink files  ──►  /Volumes/<catalog>/<landing schema>/files/telematics/<time>_<chassis>.json
                                        │  Auto Loader (cloudFiles, text), streaming
                                        ▼
                         telematics_raw  ──►  telematics
```

- **Volume:** `resources/databricks_end_to_end_project.volume.yml` defines the managed volume `files` in the landing schema.
- **Switch:** the bundle variable `telematics_source` (`files` by default, or `kafka`) drives both the simulator job (`--sink`) and the pipeline configuration (`telematics.source`).
- **Same parsing in both modes:** in `files` mode, `telematics_raw` reshapes each file line into Kafka's columns. `value` becomes the line as bytes, `topic` becomes the configured topic, `partition` and `offset` are null, and `timestamp` is the file modification time. So `telematics` and `parse_telematics` don't change.
- **The simulator's file sink:** `--sink files --landing-path` writes one file per event through the Databricks SDK Files API. This works locally through the CLI profile and inside the job.
- **Switching to Kafka later:** change the variable, then run a full refresh of `telematics_raw`, because a streaming table's progress is tied to its source.

## Error handling

- **Placeholder credentials:** running the pipeline or a non-dry-run simulator with placeholders fails with a Kafka connection or authentication error. This is expected until real values are set.
- **Stuck simulator:** the simulator flushes the producer at the end, and it exits with a clear error if a delivery fails, so it never hangs silently.

## Testing

> **Update (2026-09-28):** you chose to skip automated tests after Tasks 3–4. Only `telematics_events_test.py` and `telematics_kafka_config_test.py` exist. The parsing and simulator tests below were not written.

pytest tests follow the existing flat `tests/*_test.py` convention, and each test is written first and seen failing before the code is written:

- **`tests/telematics_events_test.py`:** every field is present, the values fall in range, and the JSON round-trips
- **`tests/telematics_kafka_config_test.py`:** the options and producer config contain the right keys and values
- **`tests/telematics_parsing_test.py`:** a fake Kafka row, with bytes in `value`, parses into the expected columns. It uses the `spark` fixture (Databricks Connect).
- **`tests/telematics_simulator_test.py`:** `run` sends `count` events keyed by `chassis_number` through a fake `send`, and `--dry-run` prints events without Kafka.

There's no end-to-end Kafka test while the credentials are placeholders.
