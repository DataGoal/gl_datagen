# Databricks notebook source
# MAGIC %md
# MAGIC # GL DataGen — Incremental Fact Load
# MAGIC
# MAGIC Appends a new batch of rows to `general_ledger_fact` **without touching**
# MAGIC any dimension table.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Run notebook `01_run_datagen` with **RUN_MODE = initial** at least once
# MAGIC before using this notebook.  The initial run generates all dimension tables
# MAGIC and saves the incremental state that this notebook reads.
# MAGIC
# MAGIC ## Key guarantees
# MAGIC | Property | Mechanism |
# MAGIC |---|---|
# MAGIC | PK uniqueness | Each batch uses PKs `(last_pk+1 .. last_pk+rows)` read from state |
# MAGIC | FK validity | FK bounds come from the frozen `dim_rows` snapshot in state |
# MAGIC | Reproducibility | Seed = `initial_seed + batch_number`; re-running batch N always produces the same rows |
# MAGIC | Partial-failure safety | PK offset is `MAX(state_pk, actual_table_max_pk)` before each batch |
# MAGIC
# MAGIC ## Scaling guide (incremental batches)
# MAGIC | Batch rows   | Workers | Partitions | Est. time |
# MAGIC |-------------|---------|------------|-----------|
# MAGIC | 500 million  | 16      | 200        | ~15 min   |
# MAGIC | 1 billion    | 16      | 400        | ~30 min   |
# MAGIC | 5 billion    | 32      | 1000       | ~2–3 hr   |

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
dbutils.widgets.text("BATCH_ROWS",       "500_000_000", "Rows per Incremental Batch")
dbutils.widgets.text("BATCH_PARTITIONS", "200",         "Output Partitions")

BATCH_ROWS       = int(dbutils.widgets.get("BATCH_ROWS").replace("_", ""))
BATCH_PARTITIONS = int(dbutils.widgets.get("BATCH_PARTITIONS").replace("_", ""))

print(f"Batch rows       : {BATCH_ROWS:,}")
print(f"Batch partitions : {BATCH_PARTITIONS}")

# COMMAND ----------

# MAGIC %md ## 2. Inspect current state

# COMMAND ----------

from src.orchestrator import DataGenOrchestrator

orch = DataGenOrchestrator(
    schema_path  = f"{REPO_ROOT}/configs/schema.yaml",
    volumes_path = f"{REPO_ROOT}/configs/data_volumes.yaml",
    spark        = spark,
)

state = orch.get_incremental_state()
if state is None:
    raise RuntimeError(
        "No incremental state found.  "
        "Run notebook 01_run_datagen with RUN_MODE='initial' first."
    )

print("Current incremental state:")
print(f"  last_fact_pk    : {state['last_fact_pk']:,}")
print(f"  total_fact_rows : {state['total_fact_rows']:,}")
print(f"  batch_count     : {state['batch_count']}")
print(f"  initial_seed    : {state['initial_seed']}")
print(f"\nNext batch will write PKs: {state['last_fact_pk'] + 1:,} .. {state['last_fact_pk'] + BATCH_ROWS:,}")

# COMMAND ----------

# MAGIC %md ## 3. Run incremental batch

# COMMAND ----------

orch.run(
    mode="incremental",
    overrides={
        "general_ledger_fact": {
            "rows":       BATCH_ROWS,
            "partitions": BATCH_PARTITIONS,
        }
    },
)

# COMMAND ----------

# MAGIC %md ## 4. Verify state after batch

# COMMAND ----------

state_after = orch.get_incremental_state()
print("Updated incremental state:")
print(f"  last_fact_pk    : {state_after['last_fact_pk']:,}")
print(f"  total_fact_rows : {state_after['total_fact_rows']:,}")
print(f"  batch_count     : {state_after['batch_count']}")

# COMMAND ----------

# MAGIC %md ## 5. Quick row-count check

# COMMAND ----------

catalog = orch.catalog
schema  = orch.db_schema
fact_tn = f"`{catalog}`.`{schema}`.`general_ledger_fact`"

actual_count = spark.table(fact_tn).count()
print(f"general_ledger_fact row count : {actual_count:,}")
print(f"State total_fact_rows         : {state_after['total_fact_rows']:,}")

if actual_count != state_after["total_fact_rows"]:
    print("⚠️  Row count mismatch — possible partial write. "
          "Reconcile by running orch.run(mode='incremental') again; "
          "the orchestrator will use the actual table MAX PK as the safe offset.")
else:
    print("✅ Row count matches state.")

# COMMAND ----------

# MAGIC %md ## 6. FK integrity spot-check (optional)
# MAGIC
# MAGIC Samples 1 M rows from the newly appended data to verify FK correctness.
# MAGIC Skip this on very large clusters to save time; run overnight if needed.

# COMMAND ----------

results = orch.verify_integrity(sample_rows=1_000_000)

assert all(v["ok"] for v in results.get("pk_uniqueness", {}).values()), \
    "PK uniqueness violation detected"
assert all(v["ok"] for v in results.get("fk_integrity", {}).values()), \
    "FK integrity violation detected"

print("✅ All integrity checks passed.")
