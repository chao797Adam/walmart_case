# Walmart End-to-End Data Pipeline

An end-to-end ELT pipeline that syncs a PostgreSQL (Walmart dataset) source into
Databricks, transforms it through a Bronze → Silver → Gold lakehouse architecture
with **dbt**, orchestrates everything with **Airflow**, and models SCD Type 2
history for the core dimensions.

> Built as a learning project following an online course, but with several
> deliberate design changes (see [Design Decisions](#design-decisions)).

---

## Tech Stack

| Layer           | Tool                                                                        |
| --------------- | --------------------------------------------------------------------------- |
| Source database | PostgreSQL (hosted, Walmart dataset)                                        |
| Ingestion       | Batch CDC via **Databricks Jobs** (cursor: `updated_timestamp`)             |
| Lakehouse       | **Databricks** (Unity Catalog: `bronze` / `silver_t` / `silver_b` / `gold`) |
| Transformation  | **dbt** (incremental models, OBT, snapshots, tests)                         |
| Orchestration   | **Apache Airflow** (Docker Compose, Databricks SDK)                         |
| Data quality    | dbt tests (`unique`, `not_null`, `relationships`, `dbt_utils`)              |

---

## Architecture

```mermaid
flowchart TD
    A["PostgreSQL - Walmart source"] -->|"batch CDC via updated_timestamp"| B["Bronze - raw tables in Databricks"]
    B -->|"dbt incremental"| C["Silver_t - cleaned per-table models"]
    C -->|"dbt table model - LEFT JOIN x6"| D["Silver_b - One Big Table"]
    C -->|"dbt table models"| E["Gold - facts + dimensions"]
    E -->|"dbt snapshot - timestamp strategy"| F["Snapshots - SCD Type 2 dimensions"]
    D -.->|"Airflow: dbt run"| E
```

**Flow summary**

1. **Source** — PostgreSQL holds the raw Walmart dataset
   (`orders`, `customers`, `products`, `order_items`, `stores`, `employees`).
2. **Bronze** — A Databricks Job batch-syncs rows where
   `updated_timestamp > last_checkpoint` into `bronze.*`.
3. **Silver_t** — One dbt **incremental model per table**, using
   `is_incremental()` + `updated_timestamp` as the cursor column.
4. **Silver_b** — A single **One Big Table (OBT)**, built by `LEFT JOIN`-ing
   all six `silver_t` tables around `orders`.
5. **Gold** —
   - **Facts** (`fact_orders`, `fact_order_items`) are built **directly from
     `silver_t`**, not from the OBT, to avoid inheriting the order → order_items
     row explosion.
   - **Dimensions** (`dim_customers`, `dim_products`, `dim_stores`,
     `dim_employees`) are also built from `silver_t`, each deduplicated with:

     ```sql
     qualify row_number() over (
       partition by <pk> order by updated_timestamp desc
     ) = 1
     ```

6. **Snapshots** — The four dimension tables each have a dbt **snapshot**
   (SCD Type 2, `timestamp` strategy). Fact tables are intentionally **not**
   snapshotted — they represent immutable business events.

---

## Project Structure

```
walmart_proj/
├── models/
│   ├── source/          # source table definitions
│   ├── silver_t/        # per-table incremental models
│   ├── silver_b/        # OBT (one big table), joined from silver_t
│   └── gold/            # fact + dimension tables
├── snapshots/           # SCD2 snapshots for the 4 dimension tables
├── tests/               # singular / custom data quality tests
├── macros/
├── airflow/
│   └── dags/
│       └── orchestrate.py
├── dbt_project.yml
├── packages.yml
└── README.md
```

---

## Quickstart

### Prerequisites

- Docker + Docker Compose
- A Databricks workspace with:
  - A SQL warehouse or cluster
  - A Job that ingests Postgres → Bronze
- A PostgreSQL source (or access to the hosted Walmart dataset)
- Python 3.10+ (only if running dbt locally)

### 1. Clone & configure

```bash
git clone <your-repo-url>
cd walmart_proj
cp .env.example .env
```

`.env` (never commit this):

```env
DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXX
DATABRICKS_JOB_ID=123456789
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxx
POSTGRES_URL=postgresql://user:pass@host:5432/walmart
```

dbt profile (`~/.dbt/profiles.yml`):

```yaml
walmart_proj:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: walmart
      schema: gold
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
```

### 2. Run the full pipeline via Airflow

```bash
cd airflow
docker compose up -d
# Airflow UI: http://localhost:8080  (airflow / airflow)
```

Trigger the DAG `orchestrate` from the UI, or via CLI:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger orchestrate
```

### 3. Run dbt manually (optional)

```bash
dbt deps
dbt source freshness
dbt run  --select silver_t
dbt test --select silver_t
dbt run  --select silver_b
dbt test --select silver_b
dbt run  --select gold
dbt snapshot
dbt run  --select gold/fact
```

---

## Orchestration (Airflow)

The `orchestrate` DAG chains the whole flow into one observable pipeline:

```
ingest_cdc
  → clean_target
  → source_freshness
  → silver_technical
  → silver_technical_tests
  → silver_business
  → silver_business_tests
  → gold
  → gold_dimensions
  → gold_facts
```

| Task                       | What it does                                                                                                                        |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `ingest_cdc`               | Python `@task`: triggers the Databricks ingest Job via `WorkspaceClient.jobs.run_now()`, polls `get_run()` every 5s, raises on non-`SUCCESS`. |
| `clean_target`             | `@task.bash`: clears `target/` and `logs/` so stale compiled artifacts don't leak.                                                  |
| `source_freshness`         | `dbt source freshness` — fail fast if Bronze is stale.                                                                              |
| `silver_technical(_tests)` | `dbt run` + `dbt test` on `silver_t`.                                                                                               |
| `silver_business(_tests)`  | `dbt run` + `dbt test` on `silver_b` (OBT).                                                                                         |
| `gold`                     | `dbt run --select gold`.                                                                                                            |
| `gold_dimensions`          | `dbt snapshot` — SCD2 for the four dimensions.                                                                                      |
| `gold_facts`               | `dbt run --select gold/fact` — explicit final fact rebuild.                                                                         |

Tasks are chained with `>>` so a failure upstream (e.g. `source_freshness` or a
Silver test) **blocks everything downstream** — Gold is never built on stale or
broken Silver data.

<details>
<summary><b>Why the Databricks SDK instead of <code>DatabricksRunNowOperator</code>?</b></summary>

Both call the same Databricks Jobs API:

```python
# Option A — SDK (used here): explicit polling loop
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

ws = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
run = ws.jobs.run_now(job_id=JOB_ID)
while True:
    state = ws.jobs.get_run(run.run_id).state
    if state.life_cycle_state in {
        RunLifeCycleState.TERMINATED,
        RunLifeCycleState.SKIPPED,
        RunLifeCycleState.INTERNAL_ERROR,
    }:
        if state.result_state == RunResultState.SUCCESS:
            break
        raise Exception(f"Job failed: {state.result_state}")
    time.sleep(5)
```

```python
# Option B — Airflow provider operator (not used): less code, less control
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DatabricksRunNowOperator(
    task_id="trigger_ingest_walmart_job",
    databricks_conn_id="databricks_default",
    job_id=JOB_ID,
)
```

The SDK route was chosen for explicit control over the polling loop and failure
semantics. The operator route is simpler when you just want "trigger and wait."
Credentials are read from `DATABRICKS_HOST` / `DATABRICKS_TOKEN` env vars — never
hardcoded.

</details>

---

## Design Decisions

### Dimensions are built from `silver_t`, not from the OBT

The course builds dimensions like `dim_customers` by `SELECT DISTINCT` from the
OBT. This project deliberately does not, for two reasons:

- **`DISTINCT` doesn't guarantee one row per business key.** It only removes
  rows identical across *every* selected column. Any column that varies row to
  row — like `current_timestamp()` audit columns — defeats it.
- **OBT inherits row duplication.** Because `orders` joins to `order_items`
  one-to-many, every dimension column pulled from the OBT is repeated once per
  order line. `DISTINCT` treats a symptom, not the cause.

In practice, snapshotting OBT-derived dimensions produced visibly more rows
than the same snapshots built from `silver_t`, because the SCD2 `timestamp`
strategy picked up spurious "changes" from the join.

Instead, each dimension is built from its own `silver_t` table and deduplicated
explicitly:

```sql
qualify row_number() over (
  partition by <pk> order by updated_timestamp desc
) = 1
```

This keeps **facts, dimensions, and the OBT as independent, parallel outputs
of the Silver layer** — no Gold model depends on another Gold/Silver-wide
output.

### Verifying dimension uniqueness

Rather than trusting `qualify row_number()`, each dimension was manually
checked for one row per key:

```sql
select product_id, count(*)
from walmart.gold.dim_products
group by 1
having count(*) > 1;
```

An empty result confirms no duplicates. This was run against all four
dimension tables (`dim_customers`, `dim_products`, `dim_stores`,
`dim_employees`).

> **Naming note:** fact models use the `fact_*` prefix in the repo
> (`fact_orders.sql`, `fact_order_items.sql`); the `fct_*` name appears in
> older test configs. Treat `fact_*` as canonical.

---

## Data Quality

- `unique` + `not_null` on business keys: `order_id`, `product_id`,
  `order_item_id`, `customer_id`, `employee_id`, `store_id`.
- `relationships` test on `fact_order_items.product_id → dim_products.product_id`.
- `dbt_utils.accepted_range` (`min_value: 0`) on `fact_order_items.line_amount`.
- `dbt_utils.expression_is_true` (`price >= 0`) on `products_t`.
- Dimension uniqueness enforced **at the SQL level**, not by `DISTINCT`.

Run all tests:

```bash
dbt test
```

---

## Credits

- Course / inspiration: [Walmart End-to-End Data Pipeline (YouTube)](https://www.youtube.com/watch?v=ZEE-jNAthB0&t=27s)