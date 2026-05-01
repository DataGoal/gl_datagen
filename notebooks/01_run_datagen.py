# Databricks notebook source
# MAGIC %md
# MAGIC # GL DataGen — Databricks Notebook
# MAGIC
# MAGIC This notebook runs the full data generation pipeline.
# MAGIC
# MAGIC ## Quick Start
# MAGIC 1. Attach to a cluster (recommend: Standard_DS3_v2 × 8 workers for 1B rows,
# MAGIC    scale up to 16+ workers for 25B rows).
# MAGIC 2. Set the `FACT_ROWS` widget to your target row count.
# MAGIC 3. Click **Run All**.
# MAGIC
# MAGIC ## Scaling guide
# MAGIC | Target rows      | Workers | Partitions | Est. time |
# MAGIC |-----------------|---------|------------|-----------|
# MAGIC | 1 million        | 4       | 20         | ~2 min    |
# MAGIC | 100 million      | 8       | 100        | ~8 min    |
# MAGIC | 1 billion        | 16      | 400        | ~30 min   |
# MAGIC | 25 billion       | 32+     | 2000       | ~6–10 hr  |

# COMMAND ----------

# MAGIC %md ## 0. Install dependencies

# COMMAND ----------

%pip install dbldatagen pyyaml

# COMMAND ----------

# MAGIC %md ## 1. Configure

# COMMAND ----------

import sys
import os

# ── Point to the repo root (adjust if using a different mount or repo path) ──
REPO_ROOT = "/Workspace/Users/balachandar.bhagyaraj@nike.com/gl_datagen"  # ← update this path
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

# ── Databricks Widgets ────────────────────────────────────────────────────────
dbutils.widgets.text("FACT_ROWS",        "100_000_0",   "Fact Table Row Count")
dbutils.widgets.text("FACT_PARTITIONS",  "200",        "Fact Table Partitions")
dbutils.widgets.text("WRITE_MODE",       "overwrite",  "Write Mode (overwrite|append)")
dbutils.widgets.text("TABLES_TO_RUN",    "",           "Tables to run (comma-sep, blank=all)")

FACT_ROWS       = int(dbutils.widgets.get("FACT_ROWS"))
FACT_PARTITIONS = int(dbutils.widgets.get("FACT_PARTITIONS"))
WRITE_MODE      = dbutils.widgets.get("WRITE_MODE")
TABLES_TO_RUN   = dbutils.widgets.get("TABLES_TO_RUN")

# Parse table filter (empty = all tables)
tables_filter = (
    [t.strip() for t in TABLES_TO_RUN.split(",") if t.strip()]
    if TABLES_TO_RUN.strip()
    else None
)

print(f"Fact rows       : {FACT_ROWS:,}")
print(f"Fact partitions : {FACT_PARTITIONS}")
print(f"Write mode      : {WRITE_MODE}")
print(f"Tables filter   : {tables_filter or 'ALL'}")

# COMMAND ----------

# MAGIC %md ## 2. Run orchestrator

# COMMAND ----------

from src.orchestrator import DataGenOrchestrator

orch = DataGenOrchestrator(
    schema_path  = f"{REPO_ROOT}/configs/schema.yaml",
    volumes_path = f"{REPO_ROOT}/configs/data_volumes.yaml",
    spark        = spark,
)

# Override fact table volume from widget values
overrides = {
    "general_ledger_fact": {
        "rows": FACT_ROWS,
        "partitions": FACT_PARTITIONS,
        "shuffle_partitions": FACT_PARTITIONS,
    }
}

orch.run(tables=tables_filter, overrides=overrides)

# COMMAND ----------

# MAGIC %md ## 3. Quick validation

# COMMAND ----------

catalog  = orch.catalog
schema   = orch.db_schema

tables_to_check = [
    "general_ledger_fact",
    "profit_center",
    "finance_product_dim_v",
    "gl_account_dim",
    "cost_center_dim_v",
    "calendar_fiscal_period_v",
]

print("=" * 60)
print(f"{'Table':<45} {'Count':>12}")
print("=" * 60)
for t in tables_to_check:
    full_name = f"`{catalog}`.`{schema}`.`{t}`"
    try:
        cnt = spark.table(full_name).count()
        print(f"{t:<45} {cnt:>12,}")
    except Exception as e:
        print(f"{t:<45}  ERROR: {e}")

# COMMAND ----------

# MAGIC %md ## 4. Sample referential integrity check

# COMMAND ----------

# Verify every FK in a sample of general_ledger_fact rows resolves to a valid dim row
sample_sql = f"""
SELECT
    COUNT(*) AS total_rows,
    COUNT(pc.profit_center_id)          AS matched_profit_center,
    COUNT(fa.functional_area_id)        AS matched_functional_area,
    COUNT(co.company_id)                AS matched_company,
    COUNT(dt.division_id)               AS matched_division,
    COUNT(vfm.version_forecast_mapping_id) AS matched_version,
    COUNT(pd.product_id)                AS matched_product,
    COUNT(cu.finance_customer_id)       AS matched_customer,
    COUNT(ca.copa_attribution_id)       AS matched_copa,
    COUNT(adt.accounting_document_type_id) AS matched_doc_type
FROM
    (SELECT * FROM `{catalog}`.`{schema}`.`general_ledger_fact` LIMIT 100000) gl
    LEFT JOIN `{catalog}`.`{schema}`.`profit_center`            pc  ON gl.profit_center_id = pc.profit_center_id
    LEFT JOIN `{catalog}`.`{schema}`.`functional_area`          fa  ON gl.functional_area_id = fa.functional_area_id
    LEFT JOIN `{catalog}`.`{schema}`.`company_code`             co  ON gl.company_id = co.company_id
    LEFT JOIN `{catalog}`.`{schema}`.`division_text`            dt  ON gl.division_id = dt.division_id
    LEFT JOIN `{catalog}`.`{schema}`.`version_forecast_mapping` vfm ON gl.version_forecast_mapping_id = vfm.version_forecast_mapping_id
    LEFT JOIN `{catalog}`.`{schema}`.`finance_product_dim_v`    pd  ON gl.product_id = pd.product_id
    LEFT JOIN `{catalog}`.`{schema}`.`finance_customer_dim_v`   cu  ON gl.customer_id = cu.finance_customer_id
    LEFT JOIN `{catalog}`.`{schema}`.`copa_attribution_dim`     ca  ON gl.copa_attribution_id = ca.copa_attribution_id
    LEFT JOIN `{catalog}`.`{schema}`.`accounting_document_type` adt ON gl.accounting_document_type_id = adt.accounting_document_type_id
"""

display(spark.sql(sample_sql))

# COMMAND ----------

# MAGIC %md ## 5. (Optional) Scale to 25 billion rows
# MAGIC
# MAGIC To generate the full 25B row dataset, re-run with these overrides:
# MAGIC ```python
# MAGIC orch.run(
# MAGIC     tables=["general_ledger_fact"],
# MAGIC     overrides={
# MAGIC         "general_ledger_fact": {
# MAGIC             "rows": 25_000_000_000,
# MAGIC             "partitions": 2000,
# MAGIC             "shuffle_partitions": 2000,
# MAGIC         }
# MAGIC     }
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Recommended cluster for 25B rows:**
# MAGIC - Driver: 64 GB RAM
# MAGIC - Workers: 16–32 × Standard_DS4_v2 (28 GB RAM, 8 cores each)
# MAGIC - Runtime: Databricks 14.x ML or above with Delta 3.x
