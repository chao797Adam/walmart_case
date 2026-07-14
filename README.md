# Walmart Data Pipeline Project

An end-to-end data pipeline project built while following a course, using Postgres (hosted via Ghost) as the source database, Databricks as the lakehouse platform, and dbt for transformation and modeling.

## Architecture Overview

```
Postgres (Ghost-hosted)
    ↓  batch CDC (updated_timestamp cursor column)
Bronze layer (raw tables in Databricks)
    ↓  dbt incremental models
Silver layer (silver_t: cleaned individual tables)
    ↓  dbt table model (LEFT JOIN across 6 tables)
Silver layer (silver_b: one big table / OBT)
    ↓  dbt table models
Gold layer (fct_orders, fct_order_items, dim_customers, dim_products, dim_stores, dim_employees)
    ↓  dbt snapshot (SCD Type 2)
Snapshots (dim_customers_snapshot, dim_products_snapshot, dim_stores_snapshot, dim_employees_snapshot)
```

## Pipeline Steps

1. **Source**: PostgreSQL database hosted via Ghost (Tiger Data), holding the raw Walmart dataset (orders, customers, products, order_items, stores, employees).
2. **Sync to Bronze**: Data is synced from Postgres into Databricks using a **batch CDC** approach — a scheduled job queries only rows where `updated_timestamp` is greater than the last sync checkpoint, rather than full-refreshing the table each run.
3. **Silver layer (silver_t)**: Each source table gets its own dbt **incremental model**, using `updated_timestamp` as the cursor column and `is_incremental()` to only pull new/changed rows on each run.
4. **Silver layer (silver_b)**: A single "One Big Table" (OBT) model, built by `LEFT JOIN`-ing all six `silver_t` tables around `orders` as the center table (orders → customers, order_items, employees, stores independently; order_items → products).
5. **Gold layer**:
   - **Fact tables** (`fct_orders`, `fct_order_items`) are built directly from `silver_t`, not from the OBT, to avoid inheriting row duplication introduced by the one-to-many join between orders and order_items.
   - **Dimension tables** (`dim_customers`, `dim_products`, `dim_stores`, `dim_employees`) are also built directly from `silver_t`, each deduplicated with `QUALIFY ROW_NUMBER() OVER (PARTITION BY <id> ORDER BY updated_timestamp DESC) = 1` to guarantee one row per business key.
6. **Snapshots**: Each of the four dimension tables has a corresponding dbt **snapshot** (`*_snapshot`), using the `timestamp` strategy against `silver_t`, to track slowly changing history (SCD Type 2). Fact tables are intentionally excluded from snapshotting, since they represent immutable business events rather than slowly changing attributes.

## Key Difference From the Course Material

The course author builds the **dimension tables** (`dim_customers`, etc.) by selecting `DISTINCT` from the OBT (`obt_b`) instead of from the underlying `silver_t` tables.

This project deliberately avoids that approach, for two reasons:

- **`DISTINCT` doesn't guarantee one row per business key.** It only removes rows that are identical across *every* selected column. Any column that varies from row to row — such as a `current_timestamp()` audit column, or a genuinely duplicated dimension row with a different `processed_at` value — defeats it entirely, since no two rows end up being truly identical.
- **Building dimensions from the OBT inherits row duplication from the start.** Because `orders` is joined to `order_items` on a one-to-many relationship, every dimension column pulled from the OBT (like `customer_name`) is repeated once per order line. `DISTINCT` on top of that is treating a symptom, not the cause.

In practice, this showed up directly: dimension snapshots built from the OBT produced visibly more rows than the same snapshots built from `silver_t`, because the snapshot's `timestamp` strategy picked up spurious "changes" introduced by the join.

Instead, this project builds every dimension table directly from its corresponding `silver_t` table, and enforces uniqueness explicitly with:

```sql
qualify row_number() over (partition by <primary_key> order by updated_timestamp desc) = 1
```

This guarantees a true one-row-per-entity dimension table regardless of what happens downstream in the OBT, and keeps fact tables, dimension tables, and the OBT as independent, parallel outputs of the silver layer — rather than having gold-layer models depend on other gold/silver-wide outputs.

### Verifying dimension uniqueness

Rather than assuming the `qualify row_number()` logic worked as intended, each of the four gold-layer dimension tables (`dim_customers`, `dim_products`, `dim_stores`, `dim_employees`) was manually verified to have exactly one row per primary key, e.g.:

```sql
select product_id, count(*)
from walmart.gold.dim_products
group by 1
having count(*) > 1
```

An empty result set confirms no duplicate keys. This check was run against all four dimension tables to confirm they hold up under the "one row per business key" requirement — the same requirement the OBT-based `DISTINCT` approach does not reliably satisfy.

## Orchestration (Airflow)

The pipeline is orchestrated end-to-end with an Airflow DAG (`airflow/dags/orchestrate.py`), running in a local Docker Compose deployment. Rather than running each dbt command by hand, the DAG chains the full flow into a single, observable pipeline:

```
ingest_cdc → clean_target → source_freshness → silver_technical → silver_technical_tests
    → silver_business → silver_business_tests → gold → gold_dimensions → gold_facts
```

- **`ingest_cdc`**: a Python `@task` that triggers the Databricks ingestion Job via `WorkspaceClient.jobs.run_now()`, then polls `jobs.get_run()` every 5 seconds until the run reaches a terminal `life_cycle_state`, raising an exception if the result isn't `SUCCESS`. This makes the CDC sync a real blocking dependency for the rest of the DAG, rather than a fire-and-forget trigger.
- **`clean_target`**: a `@task.bash` step that clears out dbt's `target/` and `logs/` directories before each run, avoiding stale compiled artifacts from a previous run leaking into the current one.
- **`source_freshness`**: runs `dbt source freshness` to confirm the Bronze data isn't stale before building on top of it.
- **`silver_technical` / `silver_technical_tests`**: `BashOperator` tasks running `dbt run --select silver_t` followed by `dbt test --select silver_t`, so a Silver-layer test failure stops the pipeline before Gold is built on top of it.
- **`silver_business` / `silver_business_tests`**: the same run/test pairing for the `silver_b` (OBT) layer.
- **`gold`**: builds all Gold-layer models (`dbt run --select gold`).
- **`gold_dimensions`**: runs `dbt snapshot` to capture the SCD2 history for the four dimension tables.
- **`gold_facts`**: runs `dbt run --select gold/fact` to (re)build the fact tables specifically, as an explicit final step.

Tasks are chained with `>>` into a single linear dependency, so a failure anywhere upstream (e.g. a failed `source_freshness` check, or a failed Silver test) blocks everything downstream — the pipeline never silently builds Gold-layer models on top of stale or broken Silver data.

### Triggering Databricks Jobs from Airflow

The `ingest_cdc` task triggers the Databricks ingestion Job directly via the **Databricks SDK**, rather than a pre-built Airflow operator:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

ws = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
job_trigger = ws.jobs.run_now(job_id=JOB_ID)

while True:
    job_run = ws.jobs.get_run(job_trigger.run_id)
    if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
        if job_run.state.result_state == RunResultState.SUCCESS:
            break
        raise Exception(f"Job failed with state: {job_run.state.result_state}")
    time.sleep(5)
```

The alternative — Airflow's `apache-airflow-providers-databricks` package and its `DatabricksRunNowOperator` — would hand off this same trigger-and-poll logic to a pre-built operator:

```python
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
DatabricksRunNowOperator(
    task_id='trigger_ingest_walmart_job',
    databricks_conn_id='databricks_default',
    job_id=JOB_ID,
)
```

Both approaches call the same underlying Databricks Jobs API. The SDK route (used here) gives explicit control over the polling loop and failure handling, at the cost of writing and maintaining that logic by hand; the operator route trades that control for Airflow's built-in connection management and run-status polling, and is the simpler choice when the goal is just "trigger an existing Job and wait for it."

**Credentials note:** the Databricks host and token used by `WorkspaceClient` are read from environment variables (`DATABRICKS_HOST` / `DATABRICKS_TOKEN`) rather than hardcoded in the DAG file, keeping them out of both the repository and the Airflow UI's task logs.

## Project Structure

```
walmart_proj/
├── models/
│   ├── source/          # source table definitions
│   ├── silver_t/        # per-table incremental models
│   ├── silver_b/        # OBT (one big table), joined from silver_t
│   └── gold/             # fact + dimension tables
├── snapshots/            # SCD2 snapshots for the 4 dimension tables
├── tests/                 # data quality tests
└── dbt_project.yml
```

## Notes on Data Quality

- `not_null` and `unique` tests on primary keys (`order_id`, `product_id`, `order_item_id`).
- `price > 0` enforced via `dbt_utils.expression_is_true` on the `products` table.
- Dimension uniqueness enforced at the SQL level (`qualify row_number()`) rather than relying on `DISTINCT`.
