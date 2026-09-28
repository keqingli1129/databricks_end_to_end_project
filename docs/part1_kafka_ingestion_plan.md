# Part 1: Kafka telematics ingestion (step-by-step plan)

> **How this plan is run:** Claude carries out one step at a time, explains what it did and why, then **pauses** for you. Nothing is committed to git. Where a normal plan would say "commit", this one says "check `git status`", so you can see exactly which files each task touched. Steps use checkboxes (`- [ ]`).

**Goal:** stream fake car telematics events through Kafka (Confluent Cloud) into a bronze streaming table with Lakeflow Declarative Pipelines. This follows part 1 of [transcript_1.txt](transcript_1.txt).

**Architecture:** the design is in [part1_kafka_ingestion_design.md](part1_kafka_ingestion_design.md). Shared logic lives in `src/databricks_end_to_end_project/telematics/`: the event generator, the Kafka settings, the parsing, and the simulator loop. The pipeline and the simulator both reuse it. The bundle adds 4 medallion schemas (to the existing schema file), a secret scope, a new pipeline and a simulator job.

**Tech stack:**

- Databricks Declarative Automation Bundles
- Lakeflow Declarative Pipelines (`pyspark.pipelines`)
- the Spark Kafka source
- `confluent-kafka` (Python producer)
- pytest with Databricks Connect
- uv

## Global constraints

- **No new catalogs.** Use `e2e_dev` (dev) and `e2e_prod` (prod), and add only schemas.
- **No real credentials anywhere.** Secret values are placeholders: `<BOOTSTRAP_SERVER>`, `<API_KEY>` and `<API_SECRET>`. They're set only with `databricks secrets put-secret`, never in YAML or code.
- **No git commits.**
- **Profile `DEFAULT`, target `dev`.** Every CLI command passes `--profile DEFAULT` and deploys only to `dev`. Prod is out of scope for this plan.
- **Clear the ROS path for local Python.** Run local Python commands as `env -u PYTHONPATH uv run ...`, because your shell's ROS `PYTHONPATH` breaks pytest.
- **Test first.** Every code unit gets its test written first and seen failing. Tests go in the flat `tests/*_test.py` layout.

---

### Task 1: Medallion schemas

**Files:**
- Modify: `resources/databricks_end_to_end_project.schema.yml`. The four schemas are added next to the existing `databricks_end_to_end_project_schema`, so all project schemas live in one file. The CLI prints a style note, "define a single schema in a file", on every validate. That note is harmless, and we chose one file on purpose.

**Interfaces:**
- Produces the bundle resources `resources.schemas.landing`, `.bronze`, `.silver` and `.gold`. Later tasks use `${resources.schemas.bronze.name}`.

- [x] **Step 1.1: Add the schemas**

  **Do:** under `resources.schemas` in `resources/databricks_end_to_end_project.schema.yml`, after `databricks_end_to_end_project_schema`, add:

  ```yaml
      # Medallion architecture (see docs/transcript_1.txt).
      landing:
        catalog_name: ${var.catalog}
        name: landing
        comment: Raw files as they arrive, before conversion to Delta
      bronze:
        catalog_name: ${var.catalog}
        name: bronze
        comment: Source data stored as-is in Delta
      silver:
        catalog_name: ${var.catalog}
        name: silver
        comment: Cleaned and standardized data
      gold:
        catalog_name: ${var.catalog}
        name: gold
        comment: Aggregated and joined data, ready for BI
  ```

  **Why:** the transcript starts with these four layers. The bundle owns them, so they're created on deploy.

- [x] **Step 1.2: Validate**

  **Do:** `env -u PYTHONPATH databricks bundle validate --strict --profile DEFAULT`

  **Check:** no errors, just the single-schema-per-file note. The schemas resolve to `e2e_dev.dev_keqingli1129_{landing,bronze,silver,gold}`.

- [x] **Step 1.3: Deploy to dev**

  **Do:** `env -u PYTHONPATH databricks bundle deploy --profile DEFAULT`

  **Check:** the output contains `Created schemas.landing`, `schemas.bronze`, `schemas.silver` and `schemas.gold`. In the Catalog UI, after a refresh, `e2e_dev` shows `dev_keqingli1129_landing`, `…_bronze`, `…_silver` and `…_gold`.

- [x] **Step 1.4: Check the files**

  **Do:** `git status --short`

  **Check:** `resources/databricks_end_to_end_project.schema.yml` is modified, and `docs/` is new.

---

### Task 2: Kafka secret scope and topic variable

**Files:**
- Create: `resources/kafka.secret_scope.yml`
- Modify: `databricks.yml` (the `variables:` block)

**Interfaces:**
- Produces `${resources.secret_scopes.kafka.name}`, a scope holding the keys `bootstrap_servers`, `api_key` and `api_secret`.
- Produces `${var.telematics_topic}`, default `telematics`.

- [x] **Step 2.1: Create the secret scope file**

  **Do:** create `resources/kafka.secret_scope.yml`:

  ```yaml
  # Secret scope holding the Confluent Cloud connection values.
  # Only the scope is defined here; values are set with the CLI:
  #   databricks secrets put-secret <scope> <key> --string-value "<value>"
  # Keys: bootstrap_servers, api_key, api_secret.

  resources:
    secret_scopes:
      kafka:
        name: kafka_${bundle.target}
  ```

  **Why:** this is the equivalent of the transcript's Unity Catalog service credential, a governed place for credentials so they're never hardcoded. Dev and prod live in the same workspace, so the scope name includes the target to keep them from clashing.

- [x] **Step 2.2: Add the topic variable**

  **Do:** in `databricks.yml`, under `variables:`, after the `schema:` entry, add:

  ```yaml
    telematics_topic:
      description: Kafka topic the telematics simulator writes to and the pipeline reads from
      default: telematics
  ```

- [x] **Step 2.3: Validate and read the resolved scope name**

  **Do:** `env -u PYTHONPATH databricks bundle validate --strict --profile DEFAULT -o json | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["resources"]["secret_scopes"]["kafka"]["name"], d["variables"]["telematics_topic"]["value"])'`

  **Check:** it prints the scope name, e.g. `kafka_dev` (development mode may add a prefix), followed by `telematics`. **Write down the scope name**, because later steps use it.

- [x] **Step 2.4: Deploy**

  **Do:** `env -u PYTHONPATH databricks bundle deploy --profile DEFAULT`

  **Check:** the output contains `Created secret_scopes.kafka`.

- [x] **Step 2.5: Put the placeholder secrets**

  **Do:** replace `<SCOPE>` with the name from step 2.3, then run:

  ```bash
  databricks secrets put-secret <SCOPE> bootstrap_servers --string-value "<BOOTSTRAP_SERVER>" --profile DEFAULT
  databricks secrets put-secret <SCOPE> api_key --string-value "<API_KEY>" --profile DEFAULT
  databricks secrets put-secret <SCOPE> api_secret --string-value "<API_SECRET>" --profile DEFAULT
  ```

  **Check:** `databricks secrets list-secrets <SCOPE> --profile DEFAULT` lists the 3 keys. It never shows the values.

- [x] **Step 2.6: Check the files**

  **Do:** `git status --short`

  **Check:** `resources/kafka.secret_scope.yml` is new, and `databricks.yml` is modified.

---

### Task 3: Telematics event generator

**Files:**
- Create: `src/databricks_end_to_end_project/telematics/__init__.py` (empty)
- Create: `src/databricks_end_to_end_project/telematics/events.py`
- Test: `tests/telematics_events_test.py`

**Interfaces:**
- Produces:
  - `events.generate_event(rng: random.Random, chassis_numbers: list[str], now: datetime) -> dict`, with keys `chassis_number` (str), `speed` (float), `latitude` (float), `longitude` (float) and `event_timestamp` (ISO-8601 UTC string)
  - `events.to_json(event: dict) -> bytes`
  - the constants `DEFAULT_CHASSIS_NUMBERS`, `MAX_SPEED_KMH`, `LAT_RANGE` and `LON_RANGE`

- [ ] **Step 3.1: Write the failing test**

  **Do:** create `tests/telematics_events_test.py`:

  ```python
  import json
  import random
  from datetime import datetime, timezone

  from databricks_end_to_end_project.telematics import events

  NOW = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
  CHASSIS = ["CHS000001", "CHS000002"]


  def test_generate_event_has_all_fields_in_range():
      event = events.generate_event(random.Random(42), CHASSIS, NOW)

      assert set(event) == {"chassis_number", "speed", "latitude", "longitude", "event_timestamp"}
      assert event["chassis_number"] in CHASSIS
      assert 0 <= event["speed"] <= events.MAX_SPEED_KMH
      assert events.LAT_RANGE[0] <= event["latitude"] <= events.LAT_RANGE[1]
      assert events.LON_RANGE[0] <= event["longitude"] <= events.LON_RANGE[1]
      assert event["event_timestamp"] == "2026-09-27T12:00:00+00:00"


  def test_same_seed_gives_same_event():
      first = events.generate_event(random.Random(7), CHASSIS, NOW)
      second = events.generate_event(random.Random(7), CHASSIS, NOW)
      assert first == second


  def test_to_json_round_trips():
      event = events.generate_event(random.Random(1), events.DEFAULT_CHASSIS_NUMBERS, NOW)
      assert json.loads(events.to_json(event)) == event
  ```

  **Why:** this pins down exactly what one event looks like before the code exists.

- [ ] **Step 3.2: Run the test and see it fail**

  **Do:** `env -u PYTHONPATH uv run pytest tests/telematics_events_test.py -v`

  **Check:** it fails with `ModuleNotFoundError: No module named 'databricks_end_to_end_project.telematics'`.

- [ ] **Step 3.3: Write the code**

  **Do:** create an empty `src/databricks_end_to_end_project/telematics/__init__.py`, then create `src/databricks_end_to_end_project/telematics/events.py`:

  ```python
  """Fake car telematics events, used by the Kafka simulator."""

  import json
  import random
  from datetime import datetime, timezone

  # Rough bounding box around Houston, TX.
  LAT_RANGE = (29.5, 30.1)
  LON_RANGE = (-95.8, -95.0)
  MAX_SPEED_KMH = 180.0

  DEFAULT_CHASSIS_NUMBERS = [f"CHS{n:06d}" for n in range(1, 11)]


  def generate_event(rng: random.Random, chassis_numbers: list[str], now: datetime) -> dict:
      """Return one telematics reading for a random car."""
      return {
          "chassis_number": rng.choice(chassis_numbers),
          "speed": round(rng.uniform(0, MAX_SPEED_KMH), 1),
          "latitude": round(rng.uniform(*LAT_RANGE), 6),
          "longitude": round(rng.uniform(*LON_RANGE), 6),
          "event_timestamp": now.astimezone(timezone.utc).isoformat(),
      }


  def to_json(event: dict) -> bytes:
      """Serialize an event as the Kafka message value."""
      return json.dumps(event).encode("utf-8")
  ```

- [ ] **Step 3.4: Run the test and see it pass**

  **Do:** `env -u PYTHONPATH uv run pytest tests/telematics_events_test.py -v`

  **Check:** `3 passed`.

- [ ] **Step 3.5: Check the files**

  **Do:** `git status --short`

  **Check:** the new files are the `telematics/` package and `tests/telematics_events_test.py`.

---

### Task 4: Kafka connection settings

**Files:**
- Create: `src/databricks_end_to_end_project/telematics/kafka_config.py`
- Test: `tests/telematics_kafka_config_test.py`

**Interfaces:**
- Produces:
  - `kafka_config.SECRET_KEYS = ("bootstrap_servers", "api_key", "api_secret")`
  - `kafka_config.read_credentials(dbutils, scope: str) -> tuple[str, str, str]`, returning `(bootstrap_servers, api_key, api_secret)`
  - `kafka_config.spark_kafka_options(bootstrap_servers: str, api_key: str, api_secret: str, topic: str) -> dict[str, str]`
  - `kafka_config.producer_config(bootstrap_servers: str, api_key: str, api_secret: str) -> dict[str, str]`

- [ ] **Step 4.1: Write the failing test**

  **Do:** create `tests/telematics_kafka_config_test.py`:

  ```python
  from types import SimpleNamespace

  from databricks_end_to_end_project.telematics import kafka_config


  def test_spark_kafka_options_for_confluent():
      opts = kafka_config.spark_kafka_options("<BOOTSTRAP_SERVER>", "<API_KEY>", "<API_SECRET>", "telematics")

      assert opts == {
          "kafka.bootstrap.servers": "<BOOTSTRAP_SERVER>",
          "kafka.security.protocol": "SASL_SSL",
          "kafka.sasl.mechanism": "PLAIN",
          "kafka.sasl.jaas.config": (
              "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
              'username="<API_KEY>" password="<API_SECRET>";'
          ),
          "subscribe": "telematics",
          "startingOffsets": "earliest",
      }


  def test_producer_config_for_confluent():
      conf = kafka_config.producer_config("<BOOTSTRAP_SERVER>", "<API_KEY>", "<API_SECRET>")

      assert conf == {
          "bootstrap.servers": "<BOOTSTRAP_SERVER>",
          "security.protocol": "SASL_SSL",
          "sasl.mechanisms": "PLAIN",
          "sasl.username": "<API_KEY>",
          "sasl.password": "<API_SECRET>",
      }


  def test_read_credentials_reads_the_three_keys_from_the_scope():
      calls = []

      def get(scope, key):
          calls.append((scope, key))
          return f"{key}-value"

      fake_dbutils = SimpleNamespace(secrets=SimpleNamespace(get=get))

      assert kafka_config.read_credentials(fake_dbutils, "kafka_dev") == (
          "bootstrap_servers-value",
          "api_key-value",
          "api_secret-value",
      )
      assert calls == [("kafka_dev", "bootstrap_servers"), ("kafka_dev", "api_key"), ("kafka_dev", "api_secret")]
  ```

  **Why:** Confluent Cloud needs the exact settings `SASL_SSL` + `PLAIN`. On Databricks, the login module class has a `kafkashaded.` prefix. These tests lock those settings in.

- [ ] **Step 4.2: Run the test and see it fail**

  **Do:** `env -u PYTHONPATH uv run pytest tests/telematics_kafka_config_test.py -v`

  **Check:** it fails with `ImportError: cannot import name 'kafka_config'`.

- [ ] **Step 4.3: Write the code**

  **Do:** create `src/databricks_end_to_end_project/telematics/kafka_config.py`:

  ```python
  """Connection settings for Confluent Cloud Kafka (SASL_SSL + PLAIN, API key/secret)."""

  SECRET_KEYS = ("bootstrap_servers", "api_key", "api_secret")


  def read_credentials(dbutils, scope: str) -> tuple[str, str, str]:
      """Read (bootstrap_servers, api_key, api_secret) from a Databricks secret scope."""
      servers, key, secret = (dbutils.secrets.get(scope, name) for name in SECRET_KEYS)
      return servers, key, secret


  def spark_kafka_options(bootstrap_servers: str, api_key: str, api_secret: str, topic: str) -> dict[str, str]:
      """Options for spark.readStream.format("kafka") on Databricks."""
      # Databricks ships a shaded Kafka client, hence the "kafkashaded." class prefix.
      jaas = (
          "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
          f'username="{api_key}" password="{api_secret}";'
      )
      return {
          "kafka.bootstrap.servers": bootstrap_servers,
          "kafka.security.protocol": "SASL_SSL",
          "kafka.sasl.mechanism": "PLAIN",
          "kafka.sasl.jaas.config": jaas,
          "subscribe": topic,
          "startingOffsets": "earliest",
      }


  def producer_config(bootstrap_servers: str, api_key: str, api_secret: str) -> dict[str, str]:
      """Config for confluent_kafka.Producer."""
      return {
          "bootstrap.servers": bootstrap_servers,
          "security.protocol": "SASL_SSL",
          "sasl.mechanisms": "PLAIN",
          "sasl.username": api_key,
          "sasl.password": api_secret,
      }
  ```

- [ ] **Step 4.4: Run the test and see it pass**

  **Do:** `env -u PYTHONPATH uv run pytest tests/telematics_kafka_config_test.py -v`

  **Check:** `3 passed`.

- [ ] **Step 4.5: Check the files**

  **Do:** `git status --short`

  **Check:** the new files are `kafka_config.py` and its test.

---

### Task 5: Parsing Kafka records

**Files:**
- Create: `src/databricks_end_to_end_project/telematics/parsing.py`
- Test: `tests/telematics_parsing_test.py`

**Interfaces:**
- Consumes a DataFrame with the Spark Kafka source's columns: `key` (binary), `value` (binary), `topic` (string), `partition` (int), `offset` (long), `timestamp` (timestamp).
- Produces `parsing.parse_telematics(raw_df: DataFrame) -> DataFrame`, with the string columns `chassis_number`, `speed`, `latitude`, `longitude` and `event_timestamp`, plus a `stream_metadata` struct (`topic`, `partition`, `offset`, `timestamp`) and the string `raw_json`.

- [ ] **Step 5.1: Write the failing test**

  **Do:** create `tests/telematics_parsing_test.py`:

  ```python
  from datetime import datetime

  from pyspark.sql.types import (
      BinaryType,
      IntegerType,
      LongType,
      StringType,
      StructField,
      StructType,
      TimestampType,
  )

  from databricks_end_to_end_project.telematics.parsing import parse_telematics

  # Same columns the Spark Kafka source produces.
  KAFKA_SCHEMA = StructType(
      [
          StructField("key", BinaryType()),
          StructField("value", BinaryType()),
          StructField("topic", StringType()),
          StructField("partition", IntegerType()),
          StructField("offset", LongType()),
          StructField("timestamp", TimestampType()),
      ]
  )

  VALUE = (
      b'{"chassis_number": "CHS000001", "speed": 88.5, "latitude": 29.76, '
      b'"longitude": -95.37, "event_timestamp": "2026-09-27T12:00:00+00:00"}'
  )


  def test_parse_telematics_turns_bytes_into_columns(spark):
      raw = spark.createDataFrame(
          [(b"CHS000001", VALUE, "telematics", 0, 7, datetime(2026, 9, 27, 12, 0, 1))], KAFKA_SCHEMA
      )

      row = parse_telematics(raw).collect()[0]

      assert row["chassis_number"] == "CHS000001"
      assert row["speed"] == "88.5"
      assert row["latitude"] == "29.76"
      assert row["longitude"] == "-95.37"
      assert row["event_timestamp"] == "2026-09-27T12:00:00+00:00"
      assert row["stream_metadata"]["topic"] == "telematics"
      assert row["stream_metadata"]["offset"] == 7
      assert row["raw_json"] == VALUE.decode()
  ```

  **Why:** this is the transcript's "decoding and parsing" step. Kafka hands over bytes, and we want readable columns. The test uses a fake Kafka row, so it needs no Kafka.

- [ ] **Step 5.2: Run the test and see it fail**

  **Do:** `env -u PYTHONPATH uv run pytest tests/telematics_parsing_test.py -v`

  **Check:** it fails with `ModuleNotFoundError: No module named 'databricks_end_to_end_project.telematics.parsing'`.

- [ ] **Step 5.3: Write the code**

  **Do:** create `src/databricks_end_to_end_project/telematics/parsing.py`:

  ```python
  """Turn raw Kafka records (bytes) into telematics columns."""

  from pyspark.sql import DataFrame
  from pyspark.sql import functions as F
  from pyspark.sql.types import MapType, StringType

  # Like the transcript: parse every field as a string for now; typing comes in silver.
  PAYLOAD_SCHEMA = MapType(StringType(), StringType())
  EVENT_FIELDS = ("chassis_number", "speed", "latitude", "longitude", "event_timestamp")


  def parse_telematics(raw_df: DataFrame) -> DataFrame:
      """Decode the Kafka value to JSON text and split it into one column per event field."""
      decoded = raw_df.withColumn("raw_json", F.col("value").cast("string")).withColumn(
          "payload", F.from_json("raw_json", PAYLOAD_SCHEMA)
      )
      return decoded.select(
          *[F.col("payload").getItem(field).alias(field) for field in EVENT_FIELDS],
          F.struct("topic", "partition", "offset", "timestamp").alias("stream_metadata"),
          "raw_json",
      )
  ```

- [ ] **Step 5.4: Run the test and see it pass**

  **Do:** `env -u PYTHONPATH uv run pytest tests/telematics_parsing_test.py -v`

  **Check:** `1 passed`. It takes about 20 seconds, because it uses serverless compute through Databricks Connect.

- [ ] **Step 5.5: Check the files**

  **Do:** `git status --short`

  **Check:** the new files are `parsing.py` and its test.

---

### Task 6: Simulator (local, with `--dry-run`)

**Files:**
- Create: `src/databricks_end_to_end_project/telematics/simulator.py`
- Modify: `pyproject.toml` (add to `dependencies` and `[project.scripts]`)
- Test: `tests/telematics_simulator_test.py`

**Interfaces:**
- Consumes `events.generate_event`, `events.to_json`, `events.DEFAULT_CHASSIS_NUMBERS`, `kafka_config.read_credentials` and `kafka_config.producer_config`.
- Produces:
  - `simulator.run(send, topic: str, count: int, interval: float, rng: random.Random) -> int`, where `send(topic: str, key: bytes, value: bytes)` is called once per event
  - `simulator.main(argv: list[str] | None = None)`, the `simulate` console script, with the options `--topic` (default `telematics`), `--count` (20), `--interval` (3.0), `--secret-scope` (`kafka_dev`) and `--dry-run`

- [ ] **Step 6.1: Write the failing test**

  **Do:** create `tests/telematics_simulator_test.py`:

  ```python
  import json
  import random

  from databricks_end_to_end_project.telematics import simulator


  def test_run_sends_count_events_keyed_by_chassis_number():
      sent = []

      def send(topic, key, value):
          sent.append((topic, key, value))

      assert simulator.run(send, "telematics", count=3, interval=0, rng=random.Random(1)) == 3

      assert len(sent) == 3
      for topic, key, value in sent:
          assert topic == "telematics"
          assert key.decode() == json.loads(value)["chassis_number"]


  def test_dry_run_prints_events_without_kafka(capsys):
      simulator.main(["--dry-run", "--count", "2", "--interval", "0"])

      lines = capsys.readouterr().out.strip().splitlines()
      assert len(lines) == 2
      assert all(line.startswith("[dry-run] telematics ") for line in lines)
  ```

  **Why:** the loop gets the "send" function passed in, so a test can pass a fake one, and `--dry-run` works with no Kafka at all.

- [ ] **Step 6.2: Run the test and see it fail**

  **Do:** `env -u PYTHONPATH uv run pytest tests/telematics_simulator_test.py -v`

  **Check:** it fails with `ImportError: cannot import name 'simulator'`.

- [ ] **Step 6.3: Add the dependency and the console script**

  **Do:** in `pyproject.toml`, add `"confluent-kafka>=2.6",` to the `dependencies = [...]` list, after the existing comments. Then add a line under `[project.scripts]`:

  ```toml
  simulate = "databricks_end_to_end_project.telematics.simulator:main"
  ```

  After that, run `env -u PYTHONPATH uv sync`.

  **Why:** the wheel, and so the simulator job, needs `confluent-kafka` at runtime. The `simulate` script makes `uv run simulate` work locally, and it's the wheel entry point for the job.

  **Check:** the `uv sync` output lists `+ confluent-kafka==…`.

- [ ] **Step 6.4: Write the simulator**

  **Do:** create `src/databricks_end_to_end_project/telematics/simulator.py`:

  ```python
  """Send fake telematics events to Kafka, or print them with --dry-run."""

  import argparse
  import random
  import time
  from collections.abc import Callable
  from datetime import datetime, timezone

  from databricks_end_to_end_project.telematics import events, kafka_config

  Send = Callable[[str, bytes, bytes], None]  # (topic, key, value)


  def run(send: Send, topic: str, count: int, interval: float, rng: random.Random) -> int:
      """Generate `count` events and hand each one to `send`, waiting `interval` seconds between them."""
      for i in range(count):
          event = events.generate_event(rng, events.DEFAULT_CHASSIS_NUMBERS, datetime.now(timezone.utc))
          send(topic, event["chassis_number"].encode(), events.to_json(event))
          if i < count - 1:
              time.sleep(interval)
      return count


  def _print_send(topic: str, key: bytes, value: bytes) -> None:
      print(f"[dry-run] {topic} key={key.decode()} {value.decode()}", flush=True)


  def _send_to_kafka(args: argparse.Namespace) -> None:
      from confluent_kafka import Producer
      from databricks.sdk.runtime import dbutils

      servers, key, secret = kafka_config.read_credentials(dbutils, args.secret_scope)
      producer = Producer(kafka_config.producer_config(servers, key, secret))
      errors = []

      def on_delivery(err, msg):
          if err is not None:
              errors.append(err)

      def send(topic: str, key_bytes: bytes, value: bytes) -> None:
          producer.produce(topic, key=key_bytes, value=value, on_delivery=on_delivery)
          producer.poll(0)

      run(send, args.topic, args.count, args.interval, random.Random())
      undelivered = producer.flush(30)
      if errors or undelivered:
          raise SystemExit(f"Kafka delivery failed: {undelivered} undelivered, errors: {errors[:3]}")
      print(f"Sent {args.count} events to topic {args.topic}")


  def main(argv: list[str] | None = None) -> None:
      parser = argparse.ArgumentParser(description="Send fake car telematics events to Kafka.")
      parser.add_argument("--topic", default="telematics")
      parser.add_argument("--count", type=int, default=20)
      parser.add_argument("--interval", type=float, default=3.0, help="seconds between events")
      parser.add_argument("--secret-scope", default="kafka_dev")
      parser.add_argument("--dry-run", action="store_true", help="print events instead of sending them")
      args = parser.parse_args(argv)

      if args.dry_run:
          run(_print_send, args.topic, args.count, args.interval, random.Random())
      else:
          _send_to_kafka(args)


  if __name__ == "__main__":
      main()
  ```

- [ ] **Step 6.5: Run the test and see it pass**

  **Do:** `env -u PYTHONPATH uv run pytest tests/telematics_simulator_test.py -v`

  **Check:** `2 passed`.

- [ ] **Step 6.6: Try the dry run yourself**

  **Do:** `env -u PYTHONPATH uv run simulate --dry-run --count 5 --interval 1`

  **Check:** 5 lines appear, one per second, each looking like `[dry-run] telematics key=CHS000004 {"chassis_number": "CHS000004", "speed": 97.3, ...}`.

- [ ] **Step 6.7: Try a real send and see the expected failure**

  **Do:** `env -u PYTHONPATH uv run simulate --count 1 --secret-scope <SCOPE>`, using the scope from step 2.3.

  **Check:** after about 30 seconds, it exits with `Kafka delivery failed: 1 undelivered ...`. That's expected, because `<BOOTSTRAP_SERVER>` is a placeholder. It shows the simulator read the secrets and tried to connect.

- [ ] **Step 6.8: Run the full test suite and check the files**

  **Do:** `env -u PYTHONPATH uv run pytest -v`, then `git status --short`.

  **Check:**
  - **Tests:** all pass, including the existing taxi test.
  - **Files:** new `simulator.py` and its test, with `pyproject.toml` and `uv.lock` modified.

---

### Task 7: Simulator job

**Files:**
- Create: `resources/telematics_simulator.job.yml`

**Interfaces:**
- Consumes the `simulate` entry point (Task 6), `${var.telematics_topic}` and `${resources.secret_scopes.kafka.name}` (Task 2).

- [ ] **Step 7.1: Create the job file**

  **Do:** create `resources/telematics_simulator.job.yml`:

  ```yaml
  # Runs the telematics simulator on Databricks: sends fake events to the Kafka topic.
  # No schedule; start it with: databricks bundle run telematics_simulator

  resources:
    jobs:
      telematics_simulator:
        name: telematics_simulator

        tasks:
          - task_key: simulate
            python_wheel_task:
              package_name: databricks_end_to_end_project
              entry_point: simulate
              parameters:
                - "--topic"
                - "${var.telematics_topic}"
                - "--secret-scope"
                - "${resources.secret_scopes.kafka.name}"
                - "--count"
                - "50"
                - "--interval"
                - "3"
            environment_key: default

        environments:
          - environment_key: default
            spec:
              environment_version: "5"
              dependencies:
                - ../dist/*.whl
  ```

  **Why:** this is the "run it in Databricks" half of simulator option C. It uses the same code as `uv run simulate`.

- [ ] **Step 7.2: Validate and deploy**

  **Do:** `env -u PYTHONPATH databricks bundle validate --strict --profile DEFAULT`, then `env -u PYTHONPATH databricks bundle deploy --profile DEFAULT`.

  **Check:** the deploy output contains `Created jobs.telematics_simulator`. **Don't run the job yet**, because with placeholder secrets it would just fail after 30 seconds.

- [ ] **Step 7.3: Check the files**

  **Do:** `git status --short`

  **Check:** `resources/telematics_simulator.job.yml` is new.

---

### Task 8: Pipeline with the raw Kafka table

**Files:**
- Create: `resources/telematics_ingestion.pipeline.yml`
- Create: `src/telematics_ingestion/transformations/telematics_raw.py`

**Interfaces:**
- Consumes `kafka_config.read_credentials`, `kafka_config.spark_kafka_options`, `${resources.schemas.bronze.name}` and `${resources.secret_scopes.kafka.name}`.
- Produces the streaming table `telematics_raw` in bronze, with Kafka's columns `key`, `value`, `topic`, `partition`, `offset`, `timestamp` and `timestampType`.

- [ ] **Step 8.1: Create the pipeline resource**

  **Do:** create `resources/telematics_ingestion.pipeline.yml`:

  ```yaml
  # Streaming ingestion of car telematics from Kafka (Confluent Cloud) into bronze.

  resources:
    pipelines:
      telematics_ingestion:
        name: telematics_ingestion
        catalog: ${var.catalog}
        schema: ${resources.schemas.bronze.name}
        serverless: true
        root_path: "../src/telematics_ingestion"

        configuration:
          telematics.topic: ${var.telematics_topic}
          telematics.secret_scope: ${resources.secret_scopes.kafka.name}

        libraries:
          - glob:
              include: ../src/telematics_ingestion/transformations/**

        environment:
          dependencies:
            # Installs this project's package so transformations can import databricks_end_to_end_project.
            - --editable ${workspace.file_path}
  ```

  **Why:** in the transcript this is "New ETL pipeline" in the UI, with bronze as the default schema. Here it's defined in the bundle instead. `configuration` passes the topic and scope name into the code without hardcoding them.

- [ ] **Step 8.2: Create the raw table**

  **Do:** create `src/telematics_ingestion/transformations/telematics_raw.py`:

  ```python
  from pyspark import pipelines as dp
  from databricks.sdk.runtime import dbutils

  from databricks_end_to_end_project.telematics import kafka_config

  TOPIC = spark.conf.get("telematics.topic")
  SECRET_SCOPE = spark.conf.get("telematics.secret_scope")


  @dp.table(comment="Raw Kafka records from the telematics topic; key/value are still bytes.")
  def telematics_raw():
      servers, key, secret = kafka_config.read_credentials(dbutils, SECRET_SCOPE)
      options = kafka_config.spark_kafka_options(servers, key, secret, TOPIC)
      return spark.readStream.format("kafka").options(**options).load()
  ```

  **Why:** this is the transcript's `telematics_test` table, the simplest possible read of the stream into Delta. Because it uses `readStream`, it's a **streaming table**, not a materialized view. Each run reads only the messages that are new since the last run.

- [ ] **Step 8.3: Validate and deploy**

  **Do:** `env -u PYTHONPATH databricks bundle validate --strict --profile DEFAULT`, then `env -u PYTHONPATH databricks bundle deploy --profile DEFAULT`.

  **Check:** the output contains `Created pipelines.telematics_ingestion`. The UI shows `[dev keqingli1129] telematics_ingestion` under Jobs & Pipelines.

- [ ] **Step 8.4 (optional): Run it and see the expected failure**

  **Do:** `env -u PYTHONPATH databricks bundle run telematics_ingestion --profile DEFAULT`

  **Check:** the update fails in `telematics_raw` with a Kafka error about the bootstrap server, such as `No resolvable bootstrap urls` or `Failed to construct kafka consumer`. That's expected with `<BOOTSTRAP_SERVER>`. It proves that the pipeline, the secrets and the code are all connected.

- [ ] **Step 8.5: Check the files**

  **Do:** `git status --short`

  **Check:** the new pipeline YAML and `src/telematics_ingestion/` appear.

---

### Task 9: Parsed telematics table

**Files:**
- Create: `src/telematics_ingestion/transformations/telematics.py`

**Interfaces:**
- Consumes `parsing.parse_telematics` and the streaming table `telematics_raw`.
- Produces the streaming table `telematics` in bronze.

- [ ] **Step 9.1: Create the parsed table**

  **Do:** create `src/telematics_ingestion/transformations/telematics.py`:

  ```python
  from pyspark import pipelines as dp

  from databricks_end_to_end_project.telematics.parsing import parse_telematics


  @dp.table(comment="Parsed telematics events from Kafka: one column per field, values as strings.")
  def telematics():
      return parse_telematics(spark.readStream.table("telematics_raw"))
  ```

  **Why:** this is the transcript's "decoding and parsing" table. It reads from `telematics_raw` rather than from Kafka a second time, so there's only one Kafka consumer. The pipeline graph will show `telematics_raw → telematics`.

- [ ] **Step 9.2: Deploy**

  **Do:** `env -u PYTHONPATH databricks bundle deploy --profile DEFAULT`

  **Check:** `Updated pipelines.telematics_ingestion`. The file was picked up by the `transformations/**` glob, so no YAML change was needed.

- [ ] **Step 9.3: Check the files**

  **Do:** `git status --short`

  **Check:** `telematics.py` is new.

---

### Task 10: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` (the Architecture section)

- [ ] **Step 10.1: Document the new pieces**

  **Do:** add these bullets to the Architecture list in `CLAUDE.md`, after the **Job** bullet:

  ```markdown
  - **Medallion schemas** (in `resources/databricks_end_to_end_project.schema.yml`): `landing`, `bronze`, `silver`, `gold` in `${var.catalog}`; dev prefixes them `dev_<user>_`. Reference them as `${resources.schemas.<layer>.name}`.
  - **Telematics ingestion** (docs/part1_kafka_ingestion_design.md): Confluent Cloud Kafka → `telematics_ingestion` pipeline (serverless, schema bronze) → streaming tables `telematics_raw` and `telematics`. Kafka credentials live in the bundle-defined secret scope `${resources.secret_scopes.kafka.name}` (keys `bootstrap_servers`, `api_key`, `api_secret`, set via `databricks secrets put-secret`, currently placeholders). Shared logic is in `src/databricks_end_to_end_project/telematics/`.
  - **Simulator**: `env -u PYTHONPATH uv run simulate --dry-run` prints fake events locally; without `--dry-run` it sends to Kafka using the secret scope (`--secret-scope`). The `telematics_simulator` job runs the same entry point on Databricks.
  ```

  **Check:** `git status --short` shows `CLAUDE.md` modified.

---

### Task 11 (later, optional): Switch to a real Confluent Cloud account

Do this only when you have a Confluent account. It's the only step that proves data actually flows end to end.

- [ ] **Step 11.1: Create the Kafka side in Confluent Cloud**

  **Do:**
  1. Create a **Basic** cluster, preferably on AWS `us-east-1`, near the workspace.
  2. Create the topic `telematics`.
  3. Create an **API key** for the cluster.
  4. Copy the **bootstrap server**, e.g. `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`.

- [ ] **Step 11.2: Replace the placeholder secrets**

  **Do:** run the three `put-secret` commands from step 2.5 again, with the real values.

- [ ] **Step 11.3: Send events**

  **Do:** `env -u PYTHONPATH uv run simulate --count 40 --interval 1 --secret-scope <SCOPE>`

  **Check:** `Sent 40 events to topic telematics`. The topic's message viewer in Confluent shows them.

- [ ] **Step 11.4: Run the pipeline**

  **Do:** `env -u PYTHONPATH databricks bundle run telematics_ingestion --profile DEFAULT`

  **Check:** the update completes. `telematics_raw` and `telematics` each show 40 rows written. Querying the `telematics` sample data shows readable `speed`, `latitude` and so on. **If the update can't reach Confluent at all**, look for a DNS or connection timeout rather than an authentication error. That would be the Free Edition outbound-network limit mentioned in the design.

- [ ] **Step 11.5: Continuous mode, as in the transcript**

  **Do:**
  1. Add `continuous: true` under `telematics_ingestion` in the pipeline YAML, deploy, and run.
  2. Start `uv run simulate --count 100 --interval 3` in another terminal.
  3. Watch the rows arrive live in the pipeline UI.
  4. **Stop the pipeline afterwards, and set `continuous` back to `false` and redeploy.** A continuous pipeline runs, and uses serverless compute, until you stop it.
