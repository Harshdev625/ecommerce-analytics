-- Run once in Databricks SQL editor.
-- Creates catalog, schemas, and the volume where you upload CSVs.

CREATE CATALOG IF NOT EXISTS globalmart
  COMMENT 'Olist e-commerce analytics — medallion architecture';

USE CATALOG globalmart;

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingested data';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned, typed entity tables';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Aggregations and dimensional models';

CREATE SCHEMA IF NOT EXISTS metadata
  COMMENT 'Ingestion log, DQ results, watermarks';

-- Upload your 8 CSVs here (Catalog Explorer → globalmart → bronze → raw_landing)
CREATE VOLUME IF NOT EXISTS globalmart.bronze.raw_landing
  COMMENT 'Landing zone for Olist CSV source files';

-- Optional: pipeline run summaries (JSON) — no personal paths
CREATE VOLUME IF NOT EXISTS globalmart.metadata.run_reports
  COMMENT 'Exported run summaries from notebooks';

SHOW SCHEMAS IN globalmart;
