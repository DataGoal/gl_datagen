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
dbutils.widgets.text("FACT_ROWS",        "5_000_000_000",   "Fact Table Row Count")
dbutils.widgets.text("FACT_PARTITIONS",  "400",        "Fact Table Partitions")
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

# MAGIC %md ## 4. Referential-integrity verification
# MAGIC
# MAGIC `verify_integrity()` runs three families of checks and prints a report:
# MAGIC 1. PK uniqueness on facts and dimensions.
# MAGIC 2. FK integrity from `general_ledger_fact` to every declared dim
# MAGIC    (joined on a 1M-row sample by default).
# MAGIC 3. Natural-key joinability for `CIS_fact` and
# MAGIC    `consolidated_balance_sheet_fact` (`profit_center_nbr`,
# MAGIC    `functional_area_cd`, `division_nbr`, `fiscal_year_period_nbr`).
# MAGIC
# MAGIC Every check should report `orphans=0` for the dataset to be safe to
# MAGIC build aggregate tables on top of.

# COMMAND ----------

results = orch.verify_integrity(sample_rows=1_000_000)

assert all(v["ok"] for v in results.get("pk_uniqueness", {}).values()), \
    "PK uniqueness violation detected"
assert all(v["ok"] for v in results.get("fk_integrity", {}).values()), \
    "FK integrity violation detected"
assert all(v["ok"] for v in results.get("natural_key_integrity", {}).values()), \
    "Natural-key integrity violation detected"

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
