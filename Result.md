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

`orders`, `orders_late_arrivals`, `order_items`, `customers`, `sellers`, `orders_incremental`

### Gold

`dim_date`, `dim_product`, `dim_seller`, `dim_customer`, `daily_sales_metrics`, ...

### Metadata

| Table | Purpose |
|-------|---------|
| `file_ingestion_log` | Ingestion fingerprints |
| `data_quality_results` | DQ run history |
| `dead_letter_queue` | Failed records |
| `reconciliation_log` | Bronze ↔ silver checks |
| `schema_violation_log` | Schema contract drift |
| `pipeline_watermarks` | Incremental load high-water marks |

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
| `joins_business_questions.json` | Business join questions |
| `joins_broadcast_control.json` | Broadcast join control |
| `joins_skew_distribution.json` | Skew distribution report |
| `joins_cdc_customers.json` | CDC customer MERGE simulation |
| `gold_daily_sales.json` | Gold daily sales metrics |
| `gold_customer_rfm.json` | Customer RFM segmentation |
| `gold_category_growth.json` | Category growth streaks |
| `gold_customer_summary.json` | Customer summary MERGE |
| `gold_incremental_loader.json` | Incremental loader watermarks |
| `dimensional_date_dim.json` | Date dimension |
| `dimensional_surrogate_keys.json` | Surrogate key strategy tests |
| `dimensional_product.json` | Product dimension SCD Type 1 |
| `dimensional_seller.json` | Seller dimension SCD Type 1 |
| `dimensional_customer.json` | Customer dimension SCD Type 2 |

---

## Databricks Free constraints

| Limitation | Workaround |
|------------|------------|
| `spark.conf` broadcast threshold blocked | Join hints |
| `spark.conf` adaptive/skew blocked | Let optimizer choose plan; document in report |
| Notebook runs dirty Git state | Edit locally; Pull on Databricks |

---

## Joins & CDC

### Business join questions

**Report:** `joins_business_questions.json`

| # | Question | Join | Result |
|---|----------|------|--------|
| 1 | Orders with no matching customer | `left_anti` | **0** |
| 2 | Sellers who never sold | `left_anti` | **0** |
| 3 | Top 10 delivered orders by value | `inner` | See report (top: R$13,664 — Rio de Janeiro, 8 items) |
| 4 | Customers in main + late arrivals | `inner` | **99,441** (late arrivals not empty) |

### Broadcast join control

**Report:** `joins_broadcast_control.json`  
**Pair:** `order_items` (~112k) ⋈ `sellers` (~3k)

| Variant | Strategy | Time (ms) | vs default |
|---------|----------|-----------|------------|
| Spark default | `broadcast_hash_join` | **599** | — |
| `hint("merge")` | `sort_merge_join` | 815 | 0.74× (slower) |
| `broadcast(sellers)` | `broadcast_hash_join` | 773 | 0.78× |

All plans include a shuffle on the sellers scan before broadcast — typical on Photon for small dimension tables.

### Skew distribution report

**Report:** `joins_skew_distribution.json` · **112,650** order items · threshold **3.0×**

| Column | Hottest key (prefix) | Count | Skew factor |
|--------|----------------------|-------|-------------|
| `seller_id` | `6560211a…` | 2,033 | **55.86×** |
| `product_id` | `aca2eb7d…` | 527 | **154.15×** |

All top-10 keys flagged `is_skewed: true` for both columns.

### CDC customer MERGE

**Report:** `joins_cdc_customers.json` · **Target:** `globalmart.silver.customers`

| Metric | Value |
|--------|-------|
| Batch size | **18** (6 insert + 6 update + 6 delete) |
| Row count before | 99,441 |
| Row count after | 99,441 |
| Inserts applied | **6 / 6** |
| Updates applied | **6 / 6** (city suffixed ` (CDC Updated)`) |
| Deletes removed | **6 / 6** (0 remaining) |
| **all_verified** | **true** |

MERGE handles delete, update (city/state/zip + `processed_at`), and insert on `customer_id`. Re-run `04_silver_customers_sellers.ipynb` to restore the table after testing.

---

## Gold analytics

### Daily sales metrics

**Report:** `gold_daily_sales.json` · **Table:** `globalmart.gold.daily_sales_metrics`

| Metric | Value |
|--------|-------|
| Daily rows | **612** |
| Date range | 2016-09-15 → 2018-08-29 |
| Window sizes | 7-day and 30-day moving averages |
| Sample month | January 2018 (31 days) |
| Top revenue day (Jan 2018) | **2018-01-16** — R$48,922.60 (296 orders, rank 1 in month) |

Metrics include cumulative revenue, DoD change (abs + %), and within-month revenue rank.

### Customer RFM

**Report:** `gold_customer_rfm.json` · **Table:** `globalmart.gold.customer_rfm` · **Reference date:** 2018-08-29

| Metric | Value |
|--------|-------|
| Customers scored | **96,478** |
| Top spender | R$13,664.08 (1 order — At Risk by recency) |

| Segment | Customers | Avg spend (R$) | Avg recency (days) |
|---------|-----------|----------------|--------------------|
| Potential | 38,592 | 163.77 | 92 |
| At Risk | 38,590 | 160.56 | 397 |
| Loyal | 11,699 | 70.83 | 222 |
| Needs Attention | 7,597 | 273.10 | 223 |

No **Champions** or **Lost** segments — most customers have exactly one Olist order (`frequency = 1`), so F-score quintiles collapse and high-R/high-M thresholds rarely co-occur.

### Category growth streaks

**Report:** `gold_category_growth.json` · **Table:** `globalmart.gold.category_growth_streaks`

| Metric | Value |
|--------|-------|
| Min streak length | **3** consecutive months of positive MoM revenue growth |
| Qualifying streaks | **69** |
| Longest streak | **7 months** — `construction_tools_construction` (2017-05 → 2017-11, +5,654%) |
| Notable 6-month streaks | `auto`, `cool_stuff`, `furniture_bedroom`, `health_beauty` |

### Customer summary MERGE

**Report:** `gold_customer_summary.json` · **Table:** `globalmart.gold.customer_summary` · **Reference date:** 2018-08-29

| Pass | Inactive rule | Inserts | Updates | Soft-deleted | Active | Inactive |
|------|---------------|---------|---------|--------------|--------|----------|
| 1 — initial load | 9,999 days (all active) | **96,478** | 0 | 0 | 96,478 | 0 |
| 2 — inactivity refresh | 180 days (cutoff 2018-03-02) | 0 | **57,587** | **57,587** | 38,891 | 57,587 |

MERGE upserts lifetime metrics (`total_orders`, `total_spend`, first/last order dates, AOV) and flips `is_active` when `last_order_date` is before the cutoff.

### Incremental loader

**Report:** `gold_incremental_loader.json` · **Pipeline:** `bronze_orders_to_silver_incremental`  
**Source → target:** `bronze.orders` → `silver.orders_incremental` · **Watermark table:** `metadata.pipeline_watermarks`

| Run | Previous watermark | New watermark | In batch | New since WM | Target rows | WM advanced |
|-----|-------------------|---------------|----------|--------------|-------------|-------------|
| 1 — initial load | — | 2026-06-21 17:55:33 | **99,941** | **99,941** | 99,941 | ✓ |
| 2 — idempotent rerun | 2026-06-21 17:55:33 | 2026-06-21 17:55:33 | 99,441 | **0** | 99,941 | ✗ |

Run 2 re-reads rows within the 24-hour lookback window (99,441) but advances the watermark only when `_ingested_at` exceeds the previous high-water mark — confirming idempotent incremental behavior.

---

## Dimensional model

### Date dimension

**Report:** `dimensional_date_dim.json` · **Table:** `globalmart.gold.dim_date`

| Metric | Value |
|--------|-------|
| Date range | 2016-01-01 → 2020-12-31 |
| Row count | **1,827** (matches expected) |
| Weekend days | **522** |
| Fiscal year start | **April 1** (`fiscal_month=1`, `fiscal_quarter=1` on each Apr 1) |

Uses `date_key` (YYYYMMDD integer) as the primary key — no separate surrogate needed.

### Surrogate key strategy

**Report:** `dimensional_surrogate_keys.json` · **Test entity:** `silver.sellers` (3,095 rows)

| Strategy | Scenario | Stable? | Mismatches |
|----------|----------|---------|------------|
| `monotonically_increasing_id()` | Identical plan, same session | ✓ | 0 |
| `monotonically_increasing_id()` | Repartition 4 vs 16 | ✗ (expected) | >0 after fix |
| `row_number()` by natural key | Repeat run | ✓ | 0 |
| Hash of natural key | Repeat run | ✓ | 0 |

**Decisions:** SCD Type 1 → **hash SK** · SCD Type 2 → **monotonic sequence per version** · Date dim → **`date_key`** (no surrogate)

Note: `monotonically_increasing_id()` can look stable in a same-session micro-benchmark but shifts when partition layout changes — unsuitable for SCD Type 1 reloads.

### Product dimension (SCD Type 1)

**Report:** `dimensional_product.json` · **Table:** `globalmart.gold.dim_product`

| Metric | Value |
|--------|-------|
| Products | **32,951** (distinct) |
| SK strategy | Deterministic hash |
| Category conformance | **PASSED** — 0 orphan English categories, 0 multi-map PT→EN |

Olist bronze typos (`product_name_lenght`, `product_description_lenght`) normalized to canonical column names in the dimension.

### Seller dimension (SCD Type 1)

**Report:** `dimensional_seller.json` · **Table:** `globalmart.gold.dim_seller`

| Metric | Value |
|--------|-------|
| Sellers | **3,095** (distinct) |
| SK strategy | Deterministic hash (salt=`seller`) |

### Customer dimension (SCD Type 2)

**Report:** `dimensional_customer.json` · **Table:** `globalmart.gold.dim_customer`

| Metric | Value |
|--------|-------|
| Rows after initial load | **99,441** |
| Rows after SCD2 simulation | **99,447** (+6 versions) |
| Current customers | **99,441** |
| Customers changed | **6** (city suffix ` (SCD2 Updated)`) |
| SK strategy | Monotonic sequence per version |

Sample customer `000419c5494106c306a97b5635748086`: **v1 closed** (`Niteroi (CDC Updated)`, end 2026-06-22) → **v2 current** (`Niteroi (CDC Updated) (SCD2 Updated)`). Serverless-safe snapshot via driver `.collect()` (no `.cache()`).

### Fact sales

**Report:** `dimensional_fact_sales.json` · **Table:** `globalmart.gold.fact_sales`

| Metric | Value |
|--------|-------|
| Fact rows | **110,197** |
| Total revenue | **R$15,419,773.75** |
| FK nulls | **0** on all four dimensions |
| Row count match | **PASSED** (vs delivered silver order items) |
| Revenue match | **PASSED** |
| All validations | **PASSED** |

Dimension prep at run time: 0 orphan customers/products (dim already complete after prior merge or notebook 05 rebuild).

### Star schema query

**Report:** `dimensional_star_schema_query.json` · **Query:** fact ⋈ all four dimensions

| Filter | Value |
|--------|-------|
| Year | **2018** |
| States | **SP**, **RJ**, **MG** |
| Customers | Current versions only (`is_current = true`) |
| Summary groups | **1,298** (month × state × category) |

**Top 5 by revenue:**

| Rank | Period | State | Category | Orders | Items | Revenue (R$) | Avg delivery (days) | Late % |
|------|--------|-------|----------|--------|-------|--------------|---------------------|--------|
| 1 | 2018-08 | SP | health_beauty | 370 | 409 | 55,387.86 | 3.23 | 18.09 |
| 2 | 2018-05 | SP | watches_gifts | 231 | 248 | 50,062.63 | 5.90 | 6.45 |
| 3 | 2018-05 | SP | health_beauty | 342 | 384 | 47,596.41 | 5.25 | 8.07 |
| 4 | 2018-06 | SP | health_beauty | 366 | 421 | 47,308.26 | 3.79 | 0.95 |
| 5 | 2018-02 | SP | computers_accessories | 309 | 375 | 45,193.76 | 8.83 | 9.60 |

All **top 20** rows are **São Paulo (SP)**; leading categories include **health_beauty**, **watches_gifts**, **bed_bath_table**, and **computers_accessories**.

---

## Delta operations

### Small files & OPTIMIZE

**Report:** `delta_small_files.json` · **Table:** `globalmart.gold.fact_sales_fragmented`

| Metric | Before OPTIMIZE | After OPTIMIZE |
|--------|-----------------|----------------|
| Row count | **110,197** (matches `fact_sales`) | same |
| `num_files` | **100** (100 Spark partitions) | **1** |
| Total size | 4.65 MB | 3.28 MB |
| Avg file size | **~46 KB** (small-file problem) | **~3.28 MB** |
| Files reduced | — | **99** |

OPTIMIZE compacted 100 tiny files into a single file and reduced on-disk size (deleted-row overhead removed). **Verified** on Databricks.

### Partitioning & Z-ORDER

**Report:** `delta_partition_zorder.json` · **Table:** `globalmart.gold.fact_sales_partitioned`

| Metric | Value |
|--------|-------|
| Rows | **110,197** |
| Partition column | **`order_year_month`** |
| Z-ORDER columns | `date_key`, `product_sk`, `seller_sk` |
| Partition buckets | **23** month directories (= **23** data files) |

**Cardinality analysis (delivered orders only):**

| Candidate | Distinct values | Verdict |
|-----------|-----------------|---------|
| `year` | 3 | Too coarse for pruning |
| `order_year_month` | **23** | **Chosen** — moderate cardinality |
| `date_key` | 612 | Too granular for hive-style partitions |

**DESCRIBE DETAIL (before / after Z-ORDER):** 23 files, ~173 KB avg each, ~3.99 MB total — file count unchanged because each month-partition already had one file; Z-ORDER still colocates high-cardinality filter columns within files.

**Partition pruning:** filter `order_year_month = 201803` appears in logical plan (`Filter (order_year_month = 201803)`). **Verified.**

### VACUUM

**Report:** `delta_vacuum.json` · **Table:** `globalmart.gold.fact_sales_fragmented` · **Retention:** **168 h** (7 days)

**Versions before VACUUM:**

| Version | Operation | Notes |
|---------|-----------|-------|
| 0 | CREATE OR REPLACE TABLE AS SELECT | 100 files, **110,197** rows |
| 1 | OPTIMIZE | 100 files removed → **1** file |

**Dry run:** **0** files eligible for deletion — all versions are within the 168 h retention window (table created and optimized ~30 min before VACUUM). OPTIMIZE already compacted the 100 fragment files at v1; nothing outside retention to purge yet.

**Execute VACUUM:** history shows **VACUUM START** (v2, `numFilesToDelete: 0`) and **VACUUM END** (v3, `status: COMPLETED`, `numDeletedFiles: 0`). **Verified** — multiple versions, dry-run, execute, history entries.

### Time travel

**Report:** `delta_time_travel.json` · **Table:** `globalmart.gold.fact_sales_timeline_demo`

| Step | Total revenue (R$) |
|------|-------------------|
| Baseline (v**0**) | **15,419,773.75** (matches `fact_sales`) |
| After modifying `order_item_id` **1** | **25,067,573.75** |
| Query **VERSION AS OF 0** | **15,419,773.75** |
| After **RESTORE** to v0 | **15,419,773.75** |

`restore_matches_baseline`: **true** — time travel read and restore both recover the original aggregate. **Verified.**

### Liquid clustering

**Report:** `delta_liquid_cluster.json` · **Table:** `globalmart.gold.fact_sales_liquid_cluster`  
**Compared to:** `globalmart.gold.fact_sales_partitioned` (`08_delta_ops/02`)

| Setting | Value |
|---------|-------|
| `CLUSTER BY` | `date_key`, `product_sk` |
| Filter | `date_key = 20180315` |
| Rows appended (growth sim) | **5,509** |

**Filtered revenue query (`date_key = 20180315`):**

| Table | Revenue (R$) | Elapsed (ms) |
|-------|--------------|--------------|
| Liquid cluster | **48,505.73** | **762.5** |
| Partitioned + Z-ORDER | **48,505.73** | **1,809.5** |

Revenue matches on both layouts; liquid clustering ~**2.4×** faster on this run (timing varies by cluster load).

**DESCRIBE DETAIL:** `clustering_columns` = `[date_key, product_sk]`; **1** file before append (~3.34 MB) and after append + OPTIMIZE (~3.45 MB). **Verified.**

**Delta operations complete** — notebooks `08_delta_ops/` 01–05 verified on Databricks.

---

## dbt

**Notebook:** `09_dbt/01_dbt_setup_and_run.ipynb` · **Project:** `dbt/`

### Setup & sources

| Check | Result |
|-------|--------|
| `dbt debug` (dev) | **All checks passed** |
| `dbt debug` (prod) | **All checks passed** |
| `dbt source freshness` | **8/8 PASS** (bronze sources, `_ingested_at`) |
| Dev schema | `globalmart.dbt_dev` |
| Prod schema | `globalmart.dbt_prod` |

### Incremental fact

**Table:** `globalmart.dbt_dev_marts.fact_sales_incremental`

| Run | Row count |
|-----|-----------|
| After initial `dbt run --select fact_sales_incremental` | **110,197** |
| After second run (same data) | **110,197** |

`idempotent_rerun`: **true** — matches `gold.fact_sales` row count; merge incremental is idempotent. **Verified.**

### Customer snapshot

**Table:** `globalmart.dbt_dev.snap_customers` · **Rows:** **99,441**

dbt snapshot strategy: `timestamp` on `processed_at`, `unique_key = customer_id`. Compare to SCD2 in `gold.dim_customer`. **Verified.**

### Staging, marts, tests

Built by `dbt run` / `dbt test` in notebook:

| Layer | Objects |
|-------|---------|
| Staging | `stg_orders`, `stg_order_items`, `stg_customers`, `stg_sellers`, `stg_products` |
| Intermediate | `int_delivered_order_items` |
| Marts | `mart_daily_sales`, `mart_customer_rfm` |
| Macro | `normalize_city` / `normalize_state` (customers + sellers) |
| Tests | Schema tests + `assert_delivered_orders_positive_revenue` |

Incremental fact + snapshot verified on Databricks; confirm `dbt test` output for full test suite sign-off.

---

## Gold observability

**Folder:** `06_gold_observability/` · **Code:** `src/gold_observability/`

| Notebook | Target / output | Status |
|----------|-----------------|--------|
| `01_materialized_views.ipynb` | `gold.v_seller_daily_revenue`, `gold.t_seller_daily_revenue_materialized` | **Verified** |
| `02_gold_aggregations.ipynb` | `gold.daily_sales_summary`, `gold.seller_performance_monthly` | **Verified** |
| `03_streaming_orders.ipynb` | `gold.orders_stream` vs `bronze.orders` base rows | **Verified** |
| `04_dynamic_views.ipynb` | `gold.v_customers_masked`, `gold.v_fact_sales_by_state` | **Verified** |

### Materialized views (Serverless fallback)

**Report:** `gold_materialized_views.json` · **Filter:** seller state **SP**

| Object | Name |
|--------|------|
| Regular view | `globalmart.gold.v_seller_daily_revenue` |
| Pre-computed cache | `globalmart.gold.t_seller_daily_revenue_materialized` |
| Mode | **`delta_table_cache`** — native `CREATE MATERIALIZED VIEW` disabled on Serverless Free |

**Query timing (SUM daily_revenue WHERE seller_state = 'SP'):**

| Step | Time (ms) |
|------|-----------|
| Regular view | **3,507.24** |
| Delta table query (after refresh) | **1,344.51** |
| Delta table refresh (`CREATE OR REPLACE TABLE AS SELECT`) | **4,644.82** |

Pre-computed Delta table reads ~**2.6× faster** than the regular view on this filter; refresh cost is paid upfront on each rebuild. **Verified** on Databricks Serverless.

### Daily sales summary & seller performance

**Report:** `gold_aggregations.json`

#### Daily sales summary

**Table:** `globalmart.gold.daily_sales_summary` · **Rows:** **612**

| Check | Result |
|-------|--------|
| Rows validated | **612** |
| Validation failures | **0** |
| All rows valid | **true** |

New vs returning customer logic passed row-level validation on every day. **Verified.**

#### Seller performance (monthly)

**Table:** `globalmart.gold.seller_performance_monthly` · **Rows:** **16,068**

**Top 5 sellers — Jan 2018 (overall revenue rank):**

| Rank | Seller state | Monthly revenue (R$) | Orders | Avg delivery (days) | Late rate |
|------|--------------|------------------------|--------|---------------------|-----------|
| 1 | SP | 22,898.56 | 195 | 12.16 | 5.5% |
| 2 | SP | 22,305.09 | 158 | 8.01 | 4.3% |
| 3 | SP | 21,574.08 | 229 | 8.36 | 2.4% |
| 4 | SP | 21,237.03 | 72 | 11.14 | 2.8% |
| 5 | SP | 20,040.26 | 89 | 15.89 | 15.9% |

All top-5 sample rows are **São Paulo (SP)** with matching overall and in-state ranks. **Verified.**

### Streaming orders table

**Report:** `gold_orders_stream.json` · **Source:** `/Volumes/globalmart/bronze/raw_landing/olist_orders_dataset.csv`

| Metric | Value |
|--------|-------|
| `globalmart.gold.orders_stream` rows | **99,441** |
| `bronze.orders` total rows | **99,941** |
| `bronze.orders` base rows (landing CSV) | **99,441** |
| Schema-evolution delta | **500** (from `01_bronze/04_schema_evolution`) |
| Stream vs base match | **✓** (`counts_match`: **true**, 99,441 = 99,441) |

The stream load reads only the original Olist CSV columns (`order_id`, `customer_id`, `order_status`, `order_purchase_timestamp`) plus `_stream_ingested_at`. Total `bronze.orders` is higher because **500 evolved-schema demo rows** were appended earlier. **Verified.**

### Secure views (column masking & row-level filter)

**Report:** `gold_secure_views.json`

| Object | Name |
|--------|------|
| Masked customers view | `globalmart.gold.v_customers_masked` |
| Row-filtered fact view | `globalmart.gold.v_fact_sales_by_state` |
| Access control table | `globalmart.metadata.user_state_access` |

**Column masking:** `customer_zip_code_prefix` masked as `***` + last 3 digits in `v_customers_masked` (sample read: **3** rows). **Verified.**

**Row-level security:** `v_fact_sales_by_state` joins `fact_sales` → `dim_customer` (current) → `user_state_access`, filtered by `current_user()` email. Seed grants:

| User | Allowed states |
|------|----------------|
| `analyst@example.com` | SP, RJ |
| `manager@example.com` | MG |

Views created successfully on Databricks. **Verified.**

---

## Orchestration

**Folder:** `10_orchestration/` · **Code:** `src/orchestration/` · **Workflow:** `config/workflows/globalmart_pipeline.job.json`  
**Workspace path:** `/Workspace/Users/devh9933@gmail.com/ecommerce-analytics`

| Notebook / task | Status |
|-----------------|--------|
| `01_bronze_ingestion` | **Verified** |
| `02_quality_checks` | **Verified** |
| `03_silver_transforms` | **Verified** |
| `04_reconciliation` | **Verified** |
| `05_gold_aggregations` | **Verified** |
| `06_dimensional_refresh` | **Verified** |
| `07_visualization` | **Verified** |
| `00_run_full_pipeline` | **Verified** (end-to-end, Free Edition) |
| Databricks Workflow job | Skipped (Free Edition — `00` used instead) |
| Airflow / unit tests / Lakeview | Pending |

### Bronze ingestion

**Report:** `pipeline_bronze_ingestion.json`

| Metric | Value |
|--------|-------|
| Status | **SUCCESS** |
| Files processed | **9** |
| Ingested | **0** (idempotent) |
| Skipped | **9** (8 core CSVs + optional geolocation) |

### Quality checks

**Report:** `pipeline_quality_checks.json` · **Table:** `bronze.orders`

| Metric | Value |
|--------|-------|
| Overall status | **PASSED** |
| Critical rules | **6/6** passed |
| Failed rules | none |

### Silver transforms

**Report:** `pipeline_silver_transforms.json`

| Table | Rows |
|-------|------|
| `silver.orders` (on_time) | **0** |
| `silver.orders_late_arrivals` | **99,441** |
| `silver.order_items` | **112,650** |
| `silver.customers` | **99,441** |
| `silver.sellers` | **3,095** |

Historical orders vs recent `_ingested_at` classify as `very_late`; downstream `load_all_orders()` unions both tables for gold/dimensional.

### Reconciliation

**Report:** `pipeline_reconciliation.json` · **Target:** `silver.orders_combined` (on_time ∪ late_arrivals)

| Level | Result |
|-------|--------|
| LEVEL_1_COUNT | **passed** — 99,441 = 99,441 |
| LEVEL_2_BUCKET_HASH | **passed** — 0 mismatched buckets |
| LEVEL_3_DRILL_DOWN | **passed** — 0 diff keys |
| `all_passed` | **true** |

### Gold aggregations

**Report:** `pipeline_gold_aggregations.json`

| Table | Rows |
|-------|------|
| `gold.daily_sales_summary` | **612** (validation 0 failures) |
| `gold.seller_performance_monthly` | **16,068** |

### Dimensional refresh

**Report:** `pipeline_dimensional_refresh.json`

| Object | Rows / result |
|--------|----------------|
| `gold.dim_date` | **1,827** |
| `gold.dim_product` | **32,951** |
| `gold.dim_seller` | **3,095** |
| `gold.dim_customer` (current) | **99,441** (6 SCD2 versions demo) |
| `gold.fact_sales` | **110,197** · revenue **R$15,419,773.75** · all validations **passed** |

### Visualization

**Report:** `pipeline_visualization.json` · Dashboard datasets from `fact_sales.total_amount`. **Verified.**

### End-to-end pipeline (`00_run_full_pipeline`)

**Run ID:** `f704199a-282f-4fc6-ace5-6b1ae9c9d55d` · **Databricks Free Edition** (sequential `dbutils.notebook.run`, no Workflow job)

| Task | Status |
|------|--------|
| `bronze_ingestion` | **SUCCESS** |
| `quality_checks` | **SUCCESS** |
| `silver_transforms` | **SUCCESS** |
| `reconciliation` | **SUCCESS** |
| `gold_aggregations` | **SUCCESS** |
| `dimensional_refresh` | **SUCCESS** |
| `visualization` | **SUCCESS** |

All seven tasks green in one run. **Verified.**

### Workflow job (optional)

On Free Edition, `00_run_full_pipeline.ipynb` replaces a multi-task Workflow job. Optional failure demo: re-run `00` with `simulate_failure=silver_transforms`.

**Widgets:** `pipeline_run_id`, `dry_run`, `simulate_failure`

---

## Not yet built

| Area | Planned work |
|------|----------------|
| *(none — pipeline code complete)* | Final reflection essay (assignment) |
