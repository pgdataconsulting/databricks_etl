# Operations runbook

What to look at when a pipeline runs, fails, or has to be run again. The
examples use the catalog `etl_prod`; replace it with yours.

## Where the evidence is

| Question | Where to look |
| --- | --- |
| Did the job run, and how long did it take? | Databricks Jobs UI; failure and long-run emails from the job |
| Which step failed, and why? | The task's output (the error names pipeline, step and step type) and `etl_audit.pipeline_errors` |
| How many rows did each step process? | `etl_audit.pipeline_steps` (with `settings.log_row_counts: true`) |
| What did a run read and write? | `lineage` in the run summary; Unity Catalog lineage for tables |
| Which rows were rejected? | The quarantine table of the pipeline |
| How far has an incremental load got? | `etl_state.watermarks` |

Every log line is one JSON object with `event`, `process_name`, `run_id`,
`environment` and `timestamp`, so the driver log can be filtered by run. The
`run_id` is the orchestrator's id when one is passed (`--run-id`), which ties
the Databricks run, the log lines and the audit rows together.

### Audit tables

Enabled per pipeline with `settings.audit.enabled: true`.

```sql
-- Runs of the last day that did not succeed
SELECT process_name, run_id, status, start_time, error
FROM etl_prod.etl_audit.pipeline_runs
WHERE status <> 'succeeded' AND start_time >= current_date() - INTERVAL 1 DAY
ORDER BY start_time DESC;

-- Where the time of one run went
SELECT step_name, step_type, duration_seconds, rows_processed
FROM etl_prod.etl_audit.pipeline_steps
WHERE run_id = '<run id>'
ORDER BY duration_seconds DESC;

-- Row counts of a step over time: a sudden drop is worth a look
SELECT date(start_time) AS day, sum(rows_processed) AS rows
FROM etl_prod.etl_audit.pipeline_steps
WHERE process_name = 'medallion_orders_silver' AND step_name = 'upsert_silver_orders'
GROUP BY ALL ORDER BY day DESC;
```

Audit writes never fail a run. If the audit tables stay empty, search the
driver log for `Audit logging to ... failed`: the usual cause is a missing
grant on the audit schema.

## When a run fails

The exit code of the `run` command tells the two cases apart:

| Exit code | Meaning | Data touched? |
| --- | --- | --- |
| 2 | The configuration or the command line was rejected: invalid YAML, a missing parameter, an unknown operation, a broken reference, a malformed `--param` | No. Nothing executed, and no state table was read or created |
| 1 | A step failed while executing | Steps before the failing one have run |

**Exit code 2** is always a change problem: a config or a parameter. The
message lists every problem at once. Reproduce it without a cluster:

```bash
etl-framework validate configs/medallion/orders_silver.yaml
etl-framework run configs/medallion/orders_silver.yaml --dry-run
```

**Exit code 1** names the step. The common causes, by error type:

| Error | Usual cause | What to do |
| --- | --- | --- |
| `SchemaEnforcementError` | The source or the pipeline's output no longer matches its contract | Compare the listed columns with `schemas/…`; decide whether the contract or the data is wrong. Nothing was written by the failing step |
| `QualityCheckError` | Data broke a rule with `on_failure: fail` | Inspect the offending rows; fix upstream or relax the rule deliberately |
| `ReconciliationError` | Source and target disagree | The message has counts per category; a `reconcile` step with `output` gives the rows |
| `StepExecutionError` with a secret problem | The scope or key does not exist, or the job's identity cannot read it | Check the secret scope ACL for the runner service principal |
| `StepExecutionError` with a Spark error | Missing table or grant, unavailable source | The original Spark message is included; values of resolved secrets are replaced by `***` |

## Running again

Whether a rerun is safe depends on the sink, not on the framework:

| Sink | Rerun |
| --- | --- |
| `merge`, `scd2`, `cdc` | Safe: the same input produces the same target |
| `write` with `mode: overwrite` or `overwrite_partitions` | Safe: the same partitions are replaced |
| `write` with `mode: append` | Not safe by itself: a rerun appends again |

A failed run never advances its watermark, so rerunning an incremental
pipeline repeats exactly the window that failed. The job in this repository
retries the silver and gold tasks once for that reason.

To rerun for a past date, pass it as a job parameter:

```bash
databricks bundle run medallion_orders -t prod --params business_date=2026-07-01
```

### Reprocessing an incremental pipeline

To make a pipeline read its source again from an earlier point, move its
watermark back. The next run then picks up everything after that value.

```sql
-- See the current position
SELECT * FROM etl_prod.etl_state.watermarks WHERE process_name = 'medallion_orders_silver';

-- Reprocess everything changed since 1 July
UPDATE etl_prod.etl_state.watermarks
SET watermark = '2026-07-01 00:00:00', updated_at = current_timestamp()
WHERE process_name = 'medallion_orders_silver';

-- Or start over: the next run uses `initial`
DELETE FROM etl_prod.etl_state.watermarks WHERE process_name = 'medallion_orders_silver';
```

Do this only for pipelines with an idempotent sink.

### Quarantined rows

Rows that fail row-level checks under `on_failure: quarantine` are written to
the quarantine table and left out of the load. They are not retried
automatically. After the cause is fixed upstream, the corrected rows arrive
through the normal load (their `updated_at` is newer than the watermark); the
quarantine table is the record of what was rejected and when, and can be
truncated once reviewed.

## Changing what runs

- **A pipeline's logic**: change the YAML in a pull request. CI validates it;
  merging deploys it to `test` and runs the job there; a release deploys it
  to `prod`.
- **A schema contract**: change `schemas/…` in the same pull request as the
  pipeline change that needs it.
- **A secret**: rotate it in the secret scope (or the Key Vault behind it).
  Pipelines read it on the next run; nothing is redeployed.
- **Access**: change the Terraform variables or grants in
  `infra/terraform/stacks/governance` and apply.

### Rolling back

A deployment is a Git revision, so rolling back is deploying an earlier one:

```bash
git checkout v1.0.0
databricks bundle deploy -t prod
```

This restores the wheel, the configurations and the job definitions of that
release. It does not undo data changes; use Delta time travel
(`RESTORE TABLE … TO VERSION AS OF …`) for a table that was written wrongly.

## Upgrading the framework

1. Read the [changelog](../CHANGELOG.md) for the versions in between.
2. Run `etl-framework validate` over your configurations with the new
   version: validation is where a changed rule shows up first.
3. Deploy to `test`, let the job run, then release.
