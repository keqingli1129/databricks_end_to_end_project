# Continuous mode demo (files mode): watch telematics arrive live

This is the last demo from [transcript_1.txt](transcript_1.txt): the pipeline switches from **triggered** (runs once, processes what's new, stops) to **continuous** (keeps running and ingests new data within seconds). The simulator then sends events, and you watch the rows appear live.

It works **without Kafka**. In continuous mode, Auto Loader keeps watching the landing folder and picks up each new file as it lands.

**Assumes:**

- target `dev`, profile `DEFAULT`, run from the project root
- local Python commands run with `env -u PYTHONPATH`, because your shell's ROS path breaks them
- the state after Tasks 1–11 of [part1_kafka_ingestion_plan.md](part1_kafka_ingestion_plan.md): `telematics_source = files`, and the pipeline has already run

**Time:** about 15 minutes. **Cost:** ⚠️ **a continuous pipeline runs, and uses serverless compute, until you stop it.** Step 5 stops it, so don't skip it.

| Name | Value |
|---|---|
| Pipeline (bundle key) | `telematics_ingestion` |
| Pipeline ID | `c82a1f40-d0b5-4bd0-aa8d-b16ed2e0c14f` |
| Pipeline UI | <https://dbc-b5c9918e-c2d2.cloud.databricks.com/pipelines/c82a1f40-d0b5-4bd0-aa8d-b16ed2e0c14f?w=7474646511979556> |
| Landing folder | `/Volumes/e2e_dev/dev_keqingli1129_landing/files/telematics` |
| Tables | `e2e_dev.dev_keqingli1129_bronze.telematics_raw`, `…telematics` |

---

## 1. Note the current row count

In the SQL editor:

```sql
SELECT (SELECT count(*) FROM e2e_dev.dev_keqingli1129_bronze.telematics_raw) AS raw_rows,
       (SELECT count(*) FROM e2e_dev.dev_keqingli1129_bronze.telematics)     AS parsed_rows;
```

**Expected:** both are **60**, the result after Task 10. Write the number down, because step 4 compares against it.

- [ ] Starting count: ______

---

## 2. Make the pipeline continuous

In `resources/ingest_stream_telematics.pipeline.yml`, add `continuous: true` under `serverless: true`:

```yaml
      serverless: true
      continuous: true        # ← new: keep running and ingest new data as it arrives
```

Validate and deploy:

```bash
env -u PYTHONPATH databricks bundle validate --strict --profile DEFAULT
env -u PYTHONPATH databricks bundle deploy --profile DEFAULT
```

**Expected:** `Updated pipelines.telematics_ingestion`.

**Why no full refresh?** Switching between triggered and continuous only changes **when** the pipeline runs. It still reads the same source, so each streaming table keeps its progress, and nothing is re-read or lost.

- [ ] Deployed with `continuous: true`.

---

## 3. Start the pipeline, and let it keep running

Use `start-update`, which **returns immediately** and leaves the pipeline running:

```bash
databricks pipelines start-update c82a1f40-d0b5-4bd0-aa8d-b16ed2e0c14f --profile DEFAULT
```

You can also open the **Pipeline UI** link above and click **Start**.

`databricks bundle run telematics_ingestion` also starts it, but it follows the progress in your terminal. For a pipeline that never finishes, that's awkward. If you use it and press Ctrl+C, the pipeline keeps running anyway.

Open the **Pipeline UI** and wait until:

- the update state is **RUNNING**. The first start takes 1–2 minutes for serverless to warm up.
- the graph shows `telematics_raw → telematics`, with both flows **Running**. In triggered mode they would show *Completed*.

It stays like this. Continuous mode doesn't reach "Completed".

- [ ] The pipeline is RUNNING and both flows are running.

---

## 4. Send events and watch them arrive live

Keep the pipeline UI open in your browser. In a terminal, send 40 events, one every 3 seconds, which takes about 2½ minutes:

```bash
env -u PYTHONPATH uv run simulate --sink files \
  --landing-path /Volumes/e2e_dev/dev_keqingli1129_landing/files/telematics \
  --count 40 --interval 3
```

**Watch while it runs:**

- **Pipeline UI:** the row counts on the `telematics_raw` and `telematics` boxes go up every few seconds, without you starting anything. This is the transcript's "you see the records being updated here in real time".
- **SQL editor**, running this every 20–30 seconds:

  ```sql
  SELECT count(*) AS parsed_rows, max(event_timestamp) AS latest_event
  FROM e2e_dev.dev_keqingli1129_bronze.telematics;
  ```

  `parsed_rows` climbs toward start + 40, and `latest_event` stays within a few seconds of now.

**Expected at the end:** `Wrote 40 event files to …`. About 10–20 seconds later, both tables show **start + 40**, which is 100 if you started at 60.

Why "a few seconds" and not instant? Each file takes about 1 second to upload from your laptop. Then Auto Loader notices new files on its next check of the folder, every few seconds by default. Kafka delivers faster, because messages are pushed rather than listed. The behaviour is the same, only the delay differs.

**Optional:** instead of your laptop, run the Databricks job at the same time with `env -u PYTHONPATH databricks bundle run telematics_simulator --profile DEFAULT`, which sends 50 events at 3-second intervals. The rows then arrive with nothing running on your laptop.

- [ ] I watched the rows arrive live. Final count: ______ (= start + 40)

---

## 5. Stop the pipeline (don't skip this)

```bash
databricks pipelines stop c82a1f40-d0b5-4bd0-aa8d-b16ed2e0c14f --profile DEFAULT
```

You can also click **Stop** in the Pipeline UI.

**Check:**

```bash
databricks pipelines get c82a1f40-d0b5-4bd0-aa8d-b16ed2e0c14f --profile DEFAULT -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])'
```

**Expected:** `IDLE`. If it still says `RUNNING`, wait 30 seconds and check again, because stopping takes a moment.

- [ ] Pipeline state is IDLE.

---

## 6. Switch back to triggered, so it can't keep running by accident

Remove the `continuous: true` line from `resources/ingest_stream_telematics.pipeline.yml`, then:

```bash
env -u PYTHONPATH databricks bundle deploy --profile DEFAULT
```

**Expected:** `Updated pipelines.telematics_ingestion`.

**Check:** `git diff resources/ingest_stream_telematics.pipeline.yml` shows **no changes**, so the file is back to exactly its committed version.

Why switch back? A deploy doesn't start the pipeline. But if `continuous: true` stays in the file, any later start runs forever, and so does the `bundle run` in the plan or runbook. Triggered is the safe default for a demo project.

- [ ] Back to triggered. `git diff` on the pipeline YAML is empty.

---

## 7. What you just saw, compared with the transcript

| Transcript (Kinesis) | Here (files) |
|---|---|
| "change it from a triggered pipeline to a continuous pipeline" | step 2: `continuous: true` |
| "send a few records in real time… every 3 seconds" | step 4: `simulate --interval 3` |
| "records being updated here in real time" | step 4: counts rising in the pipeline UI |
| "Now I stopped it" | step 5: `pipelines stop` |
| no checkpoint location needed | the same. Switching triggered ↔ continuous kept each table's progress. |

---

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| Stays in `WAITING_FOR_RESOURCES` or `INITIALIZING` for a few minutes | Normal serverless cold start. Wait. |
| RUNNING, but the counts don't rise during step 4 | Check that the simulator printed `Wrote … files to /Volumes/e2e_dev/dev_keqingli1129_landing/files/telematics`, the exact folder in the pipeline config. Also check `telematics.source` is `files`: `databricks pipelines get <id> -o json \| grep telematics.source`. |
| The counts rise only in bursts | Auto Loader checks the folder every few seconds, so rows arrive in small groups. This is expected in files mode. |
| The update is **FAILED** | Open the update in the Pipeline UI and read the error on the failed table. After fixing it, start again (step 3). |
| `start-update` says an update is already running | It's already running from an earlier start, which is fine. Go to step 4. |
| You forgot step 5 and it ran for hours | Stop it now (step 5). Serverless compute was used the whole time. Check your usage in the workspace's billing or usage page. |
