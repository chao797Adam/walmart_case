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
2. **Bronze** — A per-table Auto Loader (`cloudFiles`) stream ingests new files
   from a raw Volume into `bronze.*`. See
   [Bronze Ingestion](#bronze-ingestion-auto-loader-file-level-incremental) below —
   this is **file-level** incrementality, distinct from the row-level CDC that
   happens one layer up, in Silver.
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

     See [Gold Dimensions: SCD1, Incrementally](#gold-dimensions-scd1-incrementally)
     for why these are `incremental` (not `table`) and what "SCD1" means here.
6. **Snapshots** — The four dimension tables each have a dbt **snapshot**
   (SCD Type 2, `timestamp` strategy). Fact tables are intentionally **not**
   snapshotted — they represent immutable business events. See
   [SCD1 Gold vs. SCD2 Snapshots](#scd1-gold-vs-scd2-snapshots-why-both) for why
   both exist side by side.

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

## Bronze Ingestion: Auto Loader (file-level incremental)

Each source table is loaded into Bronze with a per-table Auto Loader
(`cloudFiles`) stream, triggered `once` per Databricks Job run:

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", f"/Volumes/walmart/bronze/bronzevolume/{table_name}/checkpoint") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load(f"/Volumes/walmart/raw/rawvolume/{table_name}/")

df.writeStream.format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", f"/Volumes/walmart/bronze/bronzevolume/{table_name}/checkpoint") \
    .toTable(f"walmart.bronze.{table_name}")
```

**This is file-level incrementality, not row-level CDC.** Auto Loader's
`checkpointLocation` tracks which *files* under
`/Volumes/walmart/raw/rawvolume/{table_name}/` have already been ingested —
it has no awareness of the `updated_timestamp` column inside those files, and
it never deduplicates rows. If the same customer appears again in a later
file (e.g. an upstream full daily export), Bronze happily appends another
copy — Bronze is pure append-only history at the file level. The row-level
cursor (`updated_timestamp > max(updated_timestamp)`) only enters the
pipeline one layer up, in the `silver_t` incremental models, which is also
where the resulting duplicate rows finally get collapsed via
`qualify row_number() = 1`. Bronze's job is simply "don't re-read a file
you've already read"; Silver's job is "don't re-process a row you've
already seen, and only keep the latest version of each one."

> **Note on `.option("path", ...)`:** an earlier version of this script also
> passed `.option("path", f"/Volumes/.../{table_name}/data")` alongside
> `.toTable(...)`, intending to pin the table's storage location inside the
> Volume. On Databricks Free Edition this produced an intermittent
> `AnalysisException: Missing cloud file system scheme` — `toTable()` implies
> a UC-managed table, and giving it a `/Volumes/...` path (not a real cloud
> URI) confuses UC's temporary-credential generation for that path. Removing
> `.option("path", ...)` and letting `toTable()` fully manage storage resolved
> it. This doesn't affect incrementality — `checkpointLocation` is what drives
> incremental loading, not where the table's data physically lives.

### Verifying Bronze row counts across all six tables

```sql
select 'orders' as table_name, count(*) as row_count from walmart.bronze.orders
union all
select 'customers', count(*) from walmart.bronze.customers
union all
select 'products', count(*) from walmart.bronze.products
union all
select 'order_items', count(*) from walmart.bronze.order_items
union all
select 'stores', count(*) from walmart.bronze.stores
union all
select 'employees', count(*) from walmart.bronze.employees
order by table_name;
```

One query across all six Bronze tables, rather than checking each table
individually, to confirm every Auto Loader stream actually landed data
before trusting downstream Silver/Gold builds on top of it.

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

### Gold Dimensions: SCD1, Incrementally

`dim_customers` (and the other three dimensions) were initially built as a
`materialized='table'` model — a full rebuild off `silver_t.customers_t` on
every run:

```sql
-- before
with base_customers as (
    select ... from {{ ref('customers_t') }}
)
select ..., current_timestamp() as customer_gold_processed_at
from base_customers
qualify row_number() over (partition by customer_id order by updated_timestamp desc) = 1
```

This is correct, but not efficient. `customers_t` itself is `incremental` and
**append-only at the row-version level** — every time a customer's `email` or
`address` changes, `customers_t` gains a *new* row for that `customer_id`
rather than overwriting the old one (this is what lets the SCD2 snapshot
below reconstruct history). That means `customers_t` grows without bound as
customers get updated over time, and a `table`-materialized `dim_customers`
re-scans and re-sorts *all* of that accumulated history on every single run,
just to figure out which row is current — a cost that keeps growing.

Switched to `incremental`:

```sql
{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    merge_update_columns=['first_name','last_name','email','phone','city','province','country','updated_timestamp','is_active','processed_at','customer_gold_processed_at'],
    alias='dim_customers',
    tags=['gold', 'dim']
  )
}}
with base_customers as (
    select
        customer_id, first_name, last_name, email, phone,
        city, province, country,
        created_timestamp, updated_timestamp, is_active, processed_at
    from {{ ref('customers_t') }}
    {% if is_incremental() %}
        where updated_timestamp > (select coalesce(max(updated_timestamp), timestamp '1900-01-01') from {{ this }})
    {% endif %}
)
select
    customer_id, first_name, last_name, email, phone,
    city, province, country,
    created_timestamp, updated_timestamp, is_active, processed_at,
    current_timestamp() as customer_gold_processed_at
from base_customers
qualify row_number() over (partition by customer_id order by updated_timestamp desc) = 1
```

Now each run only scans the slice of `customers_t` newer than what's already
in `dim_customers`, and `merge_update_columns` overwrites the matched
`customer_id` in place. `qualify` still runs, but only needs to dedupe *this
run's* incoming batch, not the table's entire history.

**This makes `dim_customers` a textbook SCD Type 1 dimension**: matched keys
are updated in place via `MERGE`, old attribute values are overwritten and
not retrievable from this table. (Worth being precise about the term here —
elsewhere in this project, e.g. `fact_order_items`'s incremental merge, the
same `MERGE`-based upsert pattern exists but isn't called "SCD1," because
SCD is a dimensional-modeling term and a fact table isn't a dimension. Here
it *is* a dimension, and it *does* overwrite without history, so "SCD1"
applies correctly.)

### SCD1 Gold vs. SCD2 Snapshots: why both

`dim_customers` (SCD1, above) and `dim_customers_snapshot` (SCD2, via `dbt
snapshot`) are both built from the same `customers_t`, but they aren't
redundant — they answer two structurally different questions:

| Question | Answered by |
| --- | --- |
| "What's this customer's email *right now*?" | `dim_customers` — SCD1, one row per key, cheap to query, no history to filter through |
| "What was this customer's address 3 months ago, and when did it change?" | `dim_customers_snapshot` — SCD2, `dbt_valid_from`/`dbt_valid_to` let you query any point in time |

An SCD1 table structurally *cannot* answer the second question (no historical
rows exist to answer it with), and using the SCD2 snapshot as the default
"what's current" lookup would mean every query pays the cost of filtering
`dbt_valid_to_current` on a much larger, ever-growing table for no benefit.
Keeping both isn't double-covering the same need — it's two purpose-built
outputs off the same Silver source, each optimized for the query pattern it
serves.

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