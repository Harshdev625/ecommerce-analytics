# Result — GlobalMart Pipeline

Verified outputs from Databricks notebook runs. JSON copies live at:

```text
/Volumes/globalmart/metadata/run_reports/
```

---

## Catalog

| Object | Name |
|--------|------|
| Catalog | `globalmart` |
| Schemas | `bronze`, `silver`, `gold`, `metadata` |
| Landing volume | `globalmart.bronze.raw_landing` |
| Run reports | `globalmart.metadata.run_reports` |

---

## Bronze layer

### Idempotent ingestion

| Item | Value |
|------|-------|
| Tables | 8 CSV-derived Delta tables in `bronze` |
| Metadata | `globalmart.metadata.file_ingestion_log` |
| Idempotency | Re-running the same file adds **0 rows** |
| Report | `ingestion_latest.json` |

### Auto Loader (orders)

| Item | Value |
|------|-------|
| Table | `globalmart.bronze.orders_autoloader` |
| Idempotency | Second run adds **0 rows** |

### Nested payments

| Check | Result |
|-------|--------|
| Nested | `globalmart.bronze.order_payments_nested` |
| Flattened | `globalmart.bronze.order_payments_flattened` |
| Nested rows = distinct orders | ✓ |
| Flattened rows = source payment rows | ✓ |
| Report | `payments_nested_latest.json` |

### Schema evolution

| Item | Value |
|------|-------|
| New columns | `order_channel`, `customer_segment` |
| Appended rows | 500 (includes duplicate `order_id`s for demo) |
| Violation log | `globalmart.metadata.schema_violation_log` |

---

## Silver layer

### Data quality & dead letter queue

| Item | Value |
|------|-------|
| Checks on `bronze.orders` | 6 rules (null, unique, referential, range, …) |
| Results | `globalmart.metadata.data_quality_results` |
| DLQ | `globalmart.metadata.dead_letter_queue` |
| DLQ test | 2 synthetic bad rows |
| Note | Duplicate `order_id`s from schema evolution fail unique check on raw bronze (expected) |
| Report | `dq_latest.json` |

### Silver orders

| Table | Rows | Notes |
|-------|------|-------|
| `globalmart.silver.orders` | **0** | On-time path after quality gate |
| `globalmart.silver.orders_late_arrivals` | **99,441** | All `very_late` (historical orders vs recent ingestion) |

| Item | Value |
|------|-------|
| Reconciliation | **PASSED** — count, bucketed hash, drill-down |
| Compared against | `silver.orders` ∪ `orders_late_arrivals` |
| Reconciliation ID | `1f8c618f-26cb-43dc-ab3a-4f0868b68acc` |
| Report | `silver_orders_latest.json` |

### Silver order items

| Item | Value |
|------|-------|
| Table | `globalmart.silver.order_items` |
| Invalid prices → DLQ | **0** |
| Unknown category % | **1.44%** |
| Top untranslated categories | `null` (1,603), `portateis_cozinha_e_preparadores_de_alimentos` (15), `pc_gamer` (9) |
| Report | `silver_order_items_latest.json` |

### Silver customers & sellers

| Table | In | Out | Notes |
|-------|-----|-----|-------|
| `globalmart.silver.customers` | 99,441 | 99,441 | Duplicates removed: **0** |
| `globalmart.silver.sellers` | 3,095 | 3,095 | `seller_id` unique: **true** |
| Report | | | `silver_entities_latest.json` |

---

## Spark performance

### Execution plan diagnostics

**Report:** `m02_task21_execution_plans.json`

| Anti-pattern | Bad (ms) | Good (ms) | Speedup | Plan change |
|--------------|----------|-----------|---------|-------------|
| Filter after join + groupBy | 1,077.8 | 608.2 | **1.77×** | Filter pushed before aggregation |
| Wrong join strategy | 1,420.6 | 697.8 | **2.04×** | SortMergeJoin → BroadcastHashJoin |
| Unnecessary repartition | 1,416.3 | 1,124.7 | **1.26×** | Removed forced shuffle |

**Note:** `spark.sql.autoBroadcastJoinThreshold` not settable on Databricks Free — use `hint("merge")` vs `broadcast()`.

---

### Skew detection & remediation

**Report:** `m02_task22_skew.json`

**Skew on `silver.order_items`**

| Column | Top key (prefix) | Count | Skew factor |
|--------|------------------|-------|-------------|
| `seller_id` | `6560211a…` | 2,033 | **55.86×** |
| `product_id` | `aca2eb7d…` | 527 | **154.15×** |

Remediation used `seller_id` hot key `6560211a19b47992c3666cc44a7e94c0`, inflated **40×**.

| Approach | Time (ms) | vs baseline | Plan |
|----------|-----------|-------------|------|
| Forced sort-merge | 1,153 | — | SortMergeJoin |
| Salted (8 buckets) | 1,434 | 0.80× | Extra shuffle overhead |
| Adaptive optimizer | 978 | **1.18×** | BroadcastHashJoin |

**Note:** `spark.sql.adaptive.*` config not writable on Databricks Free.

---

### Higher-order functions vs explode

**Report:** `m02_task23_higher_order.json`  
**Source:** `order_payments_nested` (99,440 orders) · `order_payments_flattened` (103,886 lines)

| Problem | HO (ms) | Explode (ms) | Faster | HO rows | Explode rows |
|---------|---------|--------------|--------|---------|--------------|
| credit_card payment > R$100 | 942 | 1,131 | HO **1.20×** | 40,794 | 40,794 |
| Total credit_card value / order | 789 | 552 | Explode | 99,440 | 76,505 |
| Max non-boleto installments | 733 | 478 | Explode | 99,440 | 79,656 |

HO returns one row per order (zeros when no match); explode + groupBy only emits orders with matching payments.

**Key plan difference:** HO filter pushes predicate into `PhotonScan`; explode uses `GroupingAgg` on the flat table.

---

## Table inventory

### Bronze

`orders`, `order_items`, `order_payments`, `order_reviews`, `products`, `sellers`, `customers`, `product_category_name_translation`, `orders_autoloader`, `order_payments_nested`, `order_payments_flattened`

### Silver

`orders`, `orders_late_arrivals`, `order_items`, `customers`, `sellers`

### Gold

Not built yet.

### Metadata

| Table | Purpose |
|-------|---------|
| `file_ingestion_log` | Ingestion fingerprints |
| `data_quality_results` | DQ run history |
| `dead_letter_queue` | Failed records |
| `reconciliation_log` | Bronze ↔ silver checks |
| `schema_violation_log` | Schema contract drift |

---

## Run report index

| File | Area |
|------|------|
| `ingestion_latest.json` | Bronze ingestion |
| `payments_nested_latest.json` | Nested payments |
| `dq_latest.json` | Silver DQ |
| `silver_orders_latest.json` | Silver orders |
| `silver_order_items_latest.json` | Silver order items |
| `silver_entities_latest.json` | Silver customers & sellers |
| `m02_task21_execution_plans.json` | Execution plans |
| `m02_task22_skew.json` | Skew remediation |
| `m02_task23_higher_order.json` | Higher-order functions |

---

## Databricks Free constraints

| Limitation | Workaround |
|------------|------------|
| `spark.conf` broadcast threshold blocked | Join hints |
| `spark.conf` adaptive/skew blocked | Let optimizer choose plan; document in report |
| Notebook runs dirty Git state | Edit locally; Pull on Databricks |

---

## Not yet built

| Area | Planned work |
|------|----------------|
| **Joins & CDC** | Business join queries, broadcast control, customer MERGE |
| **Gold analytics** | Daily sales, RFM, seller performance, aggregations |
| **Dimensional model** | Star schema — dims + fact table |
| **Delta ops** | OPTIMIZE, partitioning, Z-order, VACUUM, time travel |
| **dbt** | Staging and mart models |
| **Orchestration** | Workflows, Airflow, unit tests, dashboard |
