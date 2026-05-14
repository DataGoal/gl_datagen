# Databricks notebook source
# MAGIC %md
# MAGIC # GL DataGen — Initial Full Load
# MAGIC
# MAGIC Generates **all** dimension and fact tables from scratch, then saves
# MAGIC incremental state so subsequent runs (notebook `02_incremental_datagen`)
# MAGIC can append new batches to `general_ledger_fact` without touching dims.
# MAGIC
# MAGIC ## Quick Start
# MAGIC 1. Attach to a cluster (Standard_DS4_v2 × 16+ workers for 25B rows).
# MAGIC 2. Set `FACT_ROWS` to your target row count.
# MAGIC 3. Leave `RUN_MODE` = **initial** for a fresh full load.
# MAGIC 4. Click **Run All**.
# MAGIC
# MAGIC ## Run-mode guide
# MAGIC | Mode        | What happens |
# MAGIC |-------------|--------------|
# MAGIC | `initial`   | All dimension + fact tables are (re)generated; incremental state is saved. |
# MAGIC | `incremental` | Only `general_ledger_fact` is appended using frozen FK bounds and continued PKs. |
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

REPO_ROOT = "/Workspace/Users/balachandar.bhagyaraj@nike.com/gl_datagen"  # ← update this path
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

# ── Databricks Widgets ────────────────────────────────────────────────────────
dbutils.widgets.dropdown("RUN_MODE",       "initial",           ["initial", "incremental"], "Run Mode")
dbutils.widgets.text("FACT_ROWS",          "25_000_000_000",    "Fact Table Row Count (initial mode)")
dbutils.widgets.text("FACT_PARTITIONS",    "1_000",             "Fact Table Partitions")
dbutils.widgets.text("INCREMENTAL_ROWS",   "500_000_000",       "Rows per Incremental Batch")
dbutils.widgets.text("TABLES_TO_RUN",      "",                  "Tables to run (comma-sep, blank=all; initial mode only)")

RUN_MODE          = dbutils.widgets.get("RUN_MODE")
FACT_ROWS         = int(dbutils.widgets.get("FACT_ROWS").replace("_", ""))
FACT_PARTITIONS   = int(dbutils.widgets.get("FACT_PARTITIONS").replace("_", ""))
INCREMENTAL_ROWS  = int(dbutils.widgets.get("INCREMENTAL_ROWS").replace("_", ""))
TABLES_TO_RUN     = dbutils.widgets.get("TABLES_TO_RUN")

tables_filter = (
    [t.strip() for t in TABLES_TO_RUN.split(",") if t.strip()]
    if TABLES_TO_RUN.strip()
    else None
)

print(f"Run mode        : {RUN_MODE}")
print(f"Fact rows       : {FACT_ROWS:,}  (initial)")
print(f"Incremental rows: {INCREMENTAL_ROWS:,}  (incremental)")
print(f"Fact partitions : {FACT_PARTITIONS}")
print(f"Tables filter   : {tables_filter or 'ALL'}  (initial mode only)")

# COMMAND ----------

# MAGIC %md ## 2. Run orchestrator

# COMMAND ----------

from src.orchestrator import DataGenOrchestrator

orch = DataGenOrchestrator(
    schema_path  = f"{REPO_ROOT}/configs/schema.yaml",
    volumes_path = f"{REPO_ROOT}/configs/data_volumes.yaml",
    spark        = spark,
)

if RUN_MODE == "initial":
    overrides = {
        "general_ledger_fact": {
            "rows":               FACT_ROWS,
            "partitions":         FACT_PARTITIONS,
            "shuffle_partitions": FACT_PARTITIONS,
        }
    }
    orch.run(tables=tables_filter, overrides=overrides, mode="initial")

elif RUN_MODE == "incremental":
    overrides = {
        "general_ledger_fact": {
            "rows":       INCREMENTAL_ROWS,
            "partitions": FACT_PARTITIONS,
        }
    }
    orch.run(overrides=overrides, mode="incremental")

else:
    raise ValueError(f"Unknown RUN_MODE: {RUN_MODE!r}.  Must be 'initial' or 'incremental'.")

# COMMAND ----------

# MAGIC %md ## 3. Incremental state

# COMMAND ----------

state = orch.get_incremental_state()
if state:
    print("Incremental state:")
    print(f"  last_fact_pk    : {state['last_fact_pk']:,}")
    print(f"  total_fact_rows : {state['total_fact_rows']:,}")
    print(f"  batch_count     : {state['batch_count']}")
    print(f"  initial_seed    : {state['initial_seed']}")
else:
    print("No incremental state found (fact table not yet generated or state table missing).")

# COMMAND ----------

# MAGIC %md ## 4. Quick validation

# COMMAND ----------

catalog = orch.catalog
schema  = orch.db_schema

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

# MAGIC %md ## 5. Referential-integrity verification
# MAGIC
# MAGIC Runs three families of checks and prints a report:
# MAGIC 1. PK uniqueness on facts and dimensions.
# MAGIC 2. FK integrity from `general_ledger_fact` to every declared dim
# MAGIC    (joined on a 1 M-row sample by default).
# MAGIC
# MAGIC Every check should report `orphans=0` for the dataset to be safe to
# MAGIC build aggregate tables on top of.

# COMMAND ----------

results = orch.verify_integrity(sample_rows=1_000_000)

assert all(v["ok"] for v in results.get("pk_uniqueness", {}).values()), \
    "PK uniqueness violation detected"
assert all(v["ok"] for v in results.get("fk_integrity", {}).values()), \
    "FK integrity violation detected"

print("✅ All integrity checks passed.")
