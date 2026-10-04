# Task 12 runbook: switch telematics ingestion from files to real Confluent Cloud Kafka

Follow this when you have a Confluent Cloud account. Everything is already built and deployed. Today, events travel as JSON files in the landing volume, and this runbook swaps that path for Kafka. **No code changes are needed**, only secrets, one variable, and a full refresh.

- **Context:** [part1_kafka_ingestion_design.md](part1_kafka_ingestion_design.md) and [part1_kafka_ingestion_plan.md](part1_kafka_ingestion_plan.md) (Tasks 1–11).
- **Assumes:** target `dev`, profile `DEFAULT`, run from the project root. Always clear the ROS path first, with `env -u PYTHONPATH`. Your shell sets it, and it breaks Python tools.

**What changes:**

```text
now:    simulate --sink files ──► /Volumes/e2e_dev/dev_keqingli1129_landing/files/telematics ──► Auto Loader ─┐
after:  simulate --sink kafka ──► Confluent topic "telematics" ──► Spark Kafka source ─────────────────────┤
                                                                                                          ▼
                                                         bronze.telematics_raw ──► bronze.telematics
```

**Time needed:** about 45 minutes. **Cost:** Confluent Basic clusters bill by usage after the free-trial credits run out. Delete the cluster when you're done (see section 9).

---

## 0. Before you start: know what you'll lose

Switching the source needs a **full refresh** of the pipeline (section 6). That **deletes all rows** currently in `telematics_raw` and `telematics`, which is 60 rows from the file simulator today. The landing JSON files stay in the volume.

If you want to keep a copy of the current rows, run this first in the SQL editor:

```sql
CREATE TABLE e2e_dev.dev_keqingli1129_bronze.telematics_files_backup AS
SELECT * FROM e2e_dev.dev_keqingli1129_bronze.telematics;
```

- [x] Decided: back up, or accept losing the file-based rows. (2026-10-04: no backup; switching back to files restores them.)

---

## 1. Create the Kafka side in Confluent Cloud

1. **Sign up or log in** at <https://confluent.cloud>.
2. **Create a cluster:** *Environments* → `default` (or create one) → **Add cluster** → **Basic**.
   - **Cloud provider:** **AWS**.
   - **Region:** **us-east-2 (Ohio)**. That's the same region as your Databricks workspace, which keeps latency low and avoids cross-region costs.
   - **Name:** e.g. `e2e-telematics`, then **Launch cluster**.
3. **Create the topic:** open the cluster → **Topics** → **Create topic**.
   - **Name:** `telematics`. It must match the bundle variable `telematics_topic`.
   - **Partitions:** `1` is enough for this demo. The default of 6 also works.
   - Skip the schema step. We send plain JSON.
4. **Create an API key:** in the cluster, go to **API Keys** → **Create key** → **My account**, the simplest option for a personal demo.
   - **Copy both the Key and the Secret now.** The secret is shown only once. Keep them in a password manager, never in the repo.
5. **Find the bootstrap server:** in the cluster, go to **Cluster settings** → **Endpoints** → **Bootstrap server**. It looks like:

   ```text
   pkc-xxxxx.us-east-2.aws.confluent.cloud:9092
   ```

- [x] I have: the **bootstrap server**, the **API key** and the **API secret**.

---

## 2. Put the real values into the Databricks secret scope

The scope `kafka_dev` already exists, created by the bundle in Task 2, and holds placeholders. Overwrite them. Put each value in single quotes, so the shell doesn't interpret special characters.

```bash
databricks secrets put-secret kafka_dev bootstrap_servers --string-value 'pkc-xxxxx.us-east-2.aws.confluent.cloud:9092' --profile DEFAULT
databricks secrets put-secret kafka_dev api_key          --string-value '<YOUR_API_KEY>'    --profile DEFAULT
databricks secrets put-secret kafka_dev api_secret       --string-value '<YOUR_API_SECRET>' --profile DEFAULT
```

**Check:**

```bash
databricks secrets list-secrets kafka_dev --profile DEFAULT
```

It should list the 3 keys with a **new** "Last Updated" timestamp. The values are never shown.

These commands go into your shell history. To keep the secret out of it, run `databricks secrets put-secret kafka_dev api_secret --profile DEFAULT` without `--string-value`, and type or paste the value when it prompts you.

- [x] Secrets updated. (2026-10-04, entered at the prompt)

---

## 3. Test Kafka from your laptop (the producer side)

This proves the credentials and topic work before Databricks is involved.

```bash
env -u PYTHONPATH uv run simulate --sink kafka --secret-scope kafka_dev --count 5 --interval 1
```

**Expected:** `Sent 5 events to topic telematics`.

**Check in Confluent:** open the topic `telematics` → **Messages**. You should see 5 JSON messages, each keyed by a chassis number like `CHS000004`.

If it fails, see **Troubleshooting** at the end. The most common causes are a typo in the bootstrap server, or an API key made for a different cluster.

- [x] 5 messages are visible in the Confluent topic. (2026-10-04: "Sent 5 events to topic telematics")

---

## 4. Test that Databricks serverless can reach Confluent (the network check)

Free Edition may restrict outbound internet access from serverless compute. Check this **before** switching the pipeline, so a network block isn't confused with a code problem.

In the workspace, create a notebook, attach it to **Serverless**, and run:

```python
import socket
host, port = dbutils.secrets.get("kafka_dev", "bootstrap_servers").rsplit(":", 1)
socket.create_connection((host, int(port)), timeout=10).close()
print("✅ reachable:", host, port)
```

- **`✅ reachable`:** continue to section 5.
- **`gaierror` or `timed out`:** serverless can't reach Confluent from this workspace. Stop here. The pipeline would fail the same way. Stay on `files` mode, or use a workspace without this restriction.

- [x] Reachable from serverless. (2026-10-04: one-time serverless notebook run printed "reachable"; a Databricks Connect UDF probe failed first with an internal sandbox error, unrelated to the network.)

---

## 5. Switch the source variable to `kafka`

Edit `databricks.yml`:

```yaml
  telematics_source:
    description: Where telematics events travel, "files" (landing volume + Auto Loader) or "kafka" (Confluent Cloud)
    default: kafka          # ← was: files
```

Validate and deploy:

```bash
env -u PYTHONPATH databricks bundle validate --strict --profile DEFAULT
env -u PYTHONPATH databricks bundle deploy --profile DEFAULT
```

**Expected:** `Updated pipelines.telematics_ingestion` and `Updated jobs.telematics_simulator`. Both pick up the new value.

**Check:**

```bash
databricks pipelines get c82a1f40-d0b5-4bd0-aa8d-b16ed2e0c14f --profile DEFAULT -o json | grep '"telematics.source"'
```

It should show `"telematics.source": "kafka"`.

To try Kafka without changing the file, use `--var="telematics_source=kafka"` on `deploy` instead. The next deploy without it switches back to `files`.

- [x] Deployed with `telematics.source = kafka`. (2026-10-04. The first deploy failed: part 6b's paused Lakebase compute blocks all deploys. Enabled it for the deploy, then disabled it again.)

---

## 6. Run the pipeline once with a full refresh

**Why a full refresh:** `telematics_raw` remembers how far it has read its **old** source, the list of files. A streaming table can't carry that progress over to a new source, and running without a refresh fails with a checkpoint or source-mismatch error. A full refresh clears both tables and their progress, then reads Kafka from the beginning, because we set `startingOffsets=earliest`.

```bash
env -u PYTHONPATH databricks bundle run telematics_ingestion --full-refresh-all --profile DEFAULT
```

**Expected:** `telematics_raw` and then `telematics` each go `RUNNING` → `COMPLETED`, then `Update … is COMPLETED.`

**Check:** run this in the SQL editor:

```sql
SELECT count(*) FROM e2e_dev.dev_keqingli1129_bronze.telematics_raw;   -- 5 (the messages from section 3)
SELECT chassis_number, speed, event_timestamp,
       stream_metadata.partition, stream_metadata.offset
FROM e2e_dev.dev_keqingli1129_bronze.telematics
ORDER BY event_timestamp;
```

`partition` and `offset` are now **filled in**. In files mode they were null, so seeing values proves the rows came from Kafka.

- [x] Kafka rows are visible in both tables. (2026-10-04: 5 / 5 rows, topic telematics, partitions 3-5 of the default 6, offsets from 0.)

---

## 6b. Full refresh of `silver.telematics` (added 2026-10-04)

The full refresh in section 6 **rewrites** `bronze.telematics`. `silver.telematics` (part 4) is a streaming table that reads it, so its checkpoint no longer matches and a normal run would fail. Refresh just that table, then do a normal run so gold recomputes:

```bash
env -u PYTHONPATH databricks bundle run transformations --full-refresh telematics --profile DEFAULT
env -u PYTHONPATH databricks bundle run transformations --profile DEFAULT
```

`--full-refresh <table>` updates only the tables it names, which is why the second, normal run is needed for the gold MVs.

- [x] silver.telematics has 5 rows; gold.aggregated_telematics has 4 cars; claim_checks has 4 claims with speed_ok checked. (2026-10-04)

---

## 7. Show that only new messages are read

```bash
env -u PYTHONPATH databricks bundle run telematics_simulator --profile DEFAULT   # 50 events over about 2.5 min, now to Kafka
env -u PYTHONPATH databricks bundle run telematics_ingestion --profile DEFAULT   # normal run, NO full refresh
```

**Check:** both tables grow by exactly **50**, so `telematics_raw` goes from 5 to 55. Kafka offsets now do the job the file list did in files mode.

- [x] +50 rows, no duplicates. (2026-10-04: the serverless simulator job sent 50; raw and bronze 5 -> 55; 55 distinct partition+offset pairs over partitions 0,1,3,4,5.)

---

## 8. Continuous mode, as in the transcript (optional)

> **2026-10-04: this section doesn't work in the `dev` target.** `mode: development` silently drops `continuous: true` from pipelines (`bundle validate` shows `continuous: None`), and `presets.trigger_pause_status: UNPAUSED` is rejected ("target with 'mode: development' cannot set trigger pause status to UNPAUSED"). In dev, the closest you can get is a **continuous job** (`continuous: pause_status: UNPAUSED` plus a `pipeline_task`), which re-runs the triggered pipeline back to back, about a minute apart; that's also what the UI's Schedule → Trigger type: Continuous creates. A truly continuous pipeline belongs in **prod**, as a target override (`targets.prod.resources.pipelines.telematics_ingestion.continuous: true`). Skipped this time.

1. In `resources/telematics_ingestion.pipeline.yml`, add under `telematics_ingestion:`:

   ```yaml
         continuous: true
   ```

   Then deploy and start the pipeline with `bundle run telematics_ingestion`, or **Start** in the UI.
2. In a second terminal, run: `env -u PYTHONPATH uv run simulate --sink kafka --count 100 --interval 3`
3. Open the pipeline in the UI. The row counts rise every few seconds, which is the transcript's "records being updated in real time".
4. **When you're done, stop it** with the **Stop** button in the UI. Then remove `continuous: true` and redeploy. A continuous pipeline runs, and uses serverless compute, until it's stopped.

- [x] Skipped (dev mode; see the note above).

---

## 9. Clean up and follow-ups

- [x] **Update `CLAUDE.md`:** in the Telematics ingestion bullet, change "(… currently placeholders)" to say the real Confluent values are set, and that the default source is `kafka`.
- [ ] **Commit** `databricks.yml` and `CLAUDE.md`. Never commit the secret values. They exist only in the secret scope.
- [ ] **Cost:** when you no longer need live Kafka, delete the Confluent cluster: *Cluster settings* → **Delete cluster**. Then switch back to files (section 10), or the pipeline will fail to connect.
- [ ] **Prod, if you ever deploy `-t prod`:** the bundle creates a **separate** scope, `kafka_prod`. Put its 3 secrets as in section 2, ideally with a separate cluster or API key for prod, then run prod's first pipeline update with `--full-refresh-all` too.

---

## 10. Switch back to files (rollback)

1. Set `telematics_source` back to `default: files` in `databricks.yml`, then deploy.
2. `env -u PYTHONPATH databricks bundle run telematics_ingestion --full-refresh-all --profile DEFAULT`

After the full refresh, Auto Loader re-reads **all** the files still in the landing volume.

---

## Troubleshooting

| Symptom | Where | Likely cause and fix |
|---|---|---|
| `Failed to resolve '…:9092': Name or service not known` | simulator or pipeline | The bootstrap server is wrong or still `<BOOTSTRAP_SERVER>`. Check `bootstrap_servers` in `kafka_dev` (section 2). |
| `SASL authentication error` / `Authentication failed` | simulator or pipeline | The API key or secret is wrong, swapped, or for a different cluster. Create a new key for **this** cluster and put both again. |
| `Topic telematics not present in metadata` / `UNKNOWN_TOPIC_OR_PART` | simulator | The topic isn't created, or its name is misspelled. It must be exactly `telematics`. |
| `Kafka delivery failed: N undelivered` after about 30 s | simulator | It couldn't connect at all. Check the section 3 steps and your laptop's network or VPN. |
| Section 4 prints `gaierror` / `timed out` | serverless notebook | Outbound network from serverless is blocked in this workspace. Stay on files mode. |
| Pipeline: `No LoginModule found` / `ClassNotFoundException … PlainLoginModule` | pipeline | In `src/databricks_end_to_end_project/telematics/kafka_config.py`, change `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule` to `org.apache.kafka.common.security.plain.PlainLoginModule`. Update `tests/telematics_kafka_config_test.py` to match, then deploy and re-run. |
| Pipeline: checkpoint, "source has changed", or schema mismatch after switching | pipeline | You ran without `--full-refresh-all`. Re-run section 6. |
| Pipeline: `No module named 'databricks_end_to_end_project'` | pipeline | `root_path` isn't `"../src"` in `telematics_ingestion.pipeline.yml`. See the step 9.4 note in the plan. |
| Pipeline completes but 0 new rows | pipeline | Nothing new is in the topic since the last run. Send events first (section 7). Or events went to a different topic: check `telematics_topic`. |
| Tables empty right after the full refresh, but the topic has messages | pipeline | `startingOffsets` isn't `earliest`, or messages older than the topic's retention period have expired. Send new events, then run again. |
