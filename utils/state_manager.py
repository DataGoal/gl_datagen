"""
utils/state_manager.py
-----------------------
Manages persistent state for incremental data generation of general_ledger_fact.

State is stored in a single-row Delta table so it survives cluster restarts
and remains consistent even after partial failures.

State fields
------------
last_fact_pk        : Highest general_ledger_fact_id written so far (0 = no data yet).
dim_rows_json       : JSON snapshot of the dim_rows map frozen at initial load time.
                      Ensures incremental FK bounds never drift from the dims on disk.
batch_count         : Number of completed incremental batches (0 after initial load).
total_fact_rows     : Cumulative row count across all loads (initial + incremental).
initial_seed        : The base seed used during the initial load.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
import pyspark.sql.types as T


_STATE_SCHEMA = T.StructType([
    T.StructField("last_fact_pk",    T.LongType(),   nullable=False),
    T.StructField("dim_rows_json",   T.StringType(), nullable=False),
    T.StructField("batch_count",     T.IntegerType(), nullable=False),
    T.StructField("total_fact_rows", T.LongType(),   nullable=False),
    T.StructField("initial_seed",    T.IntegerType(), nullable=False),
])


class IncrementalStateManager:
    """
    Tracks fact-table generation state across runs using a Delta table.

    Parameters
    ----------
    spark       : Active SparkSession.
    state_table : Fully qualified three-part Delta table name that stores state,
                  e.g. ``development.dev_pbi_perform_cf_poc_25B._datagen_state``.
    """

    def __init__(self, spark: SparkSession, state_table: str) -> None:
        self.spark = spark
        self.state_table = state_table

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def initialize(
        self,
        dim_rows: Dict[str, int],
        initial_fact_rows: int,
        seed: int = 42,
    ) -> None:
        """
        Persist state immediately after the initial full load completes.

        Parameters
        ----------
        dim_rows           : The dim_rows map that was used during the initial load.
                             This snapshot locks in FK bounds for all future batches.
        initial_fact_rows  : Total rows written to the fact table in the initial load.
        seed               : Seed used during the initial load (default 42).
        """
        row = {
            "last_fact_pk":    initial_fact_rows,
            "dim_rows_json":   json.dumps(dim_rows),
            "batch_count":     0,
            "total_fact_rows": initial_fact_rows,
            "initial_seed":    seed,
        }
        df = self.spark.createDataFrame([row], schema=_STATE_SCHEMA)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            self.state_table
        )
        print(f"[StateManager] State initialized: pk_ceiling={initial_fact_rows:,}, "
              f"dim_count={len(dim_rows)}, seed={seed}")

    def load(self) -> Dict[str, Any]:
        """
        Load current state from the Delta table.

        Returns a dict with keys:
          last_fact_pk, dim_rows (dict), batch_count, total_fact_rows, initial_seed.

        Raises
        ------
        RuntimeError
            If the state table does not exist or is empty — initial load has not run yet.
        """
        try:
            row = self.spark.table(self.state_table).first()
        except Exception as exc:
            raise RuntimeError(
                f"State table '{self.state_table}' could not be read. "
                "Run the initial full load (mode='initial') first."
            ) from exc

        if row is None:
            raise RuntimeError(
                f"State table '{self.state_table}' is empty. "
                "Run the initial full load (mode='initial') first."
            )

        return {
            "last_fact_pk":    int(row["last_fact_pk"]),
            "dim_rows":        json.loads(row["dim_rows_json"]),
            "batch_count":     int(row["batch_count"]),
            "total_fact_rows": int(row["total_fact_rows"]),
            "initial_seed":    int(row["initial_seed"]),
        }

    def update(self, added_rows: int) -> Dict[str, Any]:
        """
        Atomically advance state after a successful incremental batch.

        Parameters
        ----------
        added_rows : Number of rows written in the just-completed batch.

        Returns
        -------
        The updated state dict (same shape as ``load()``).
        """
        state = self.load()
        new_state = {
            "last_fact_pk":    state["last_fact_pk"] + added_rows,
            "dim_rows_json":   json.dumps(state["dim_rows"]),
            "batch_count":     state["batch_count"] + 1,
            "total_fact_rows": state["total_fact_rows"] + added_rows,
            "initial_seed":    state["initial_seed"],
        }
        df = self.spark.createDataFrame([new_state], schema=_STATE_SCHEMA)
        df.write.format("delta").mode("overwrite").saveAsTable(self.state_table)

        updated = {k: v for k, v in new_state.items() if k != "dim_rows_json"}
        updated["dim_rows"] = state["dim_rows"]
        print(
            f"[StateManager] State updated: batch={new_state['batch_count']}, "
            f"last_pk={new_state['last_fact_pk']:,}, total_rows={new_state['total_fact_rows']:,}"
        )
        return updated

    def exists(self) -> bool:
        """Return True if the state table exists and contains an initialized row."""
        try:
            row = self.spark.table(self.state_table).first()
            return row is not None
        except Exception:
            return False

    def get_max_pk_from_fact(self, fact_full_name: str) -> int:
        """
        Query the actual MAX PK from the fact Delta table.

        Use this as a sanity-check against the stored ``last_fact_pk`` or as a
        recovery path when state has drifted (e.g. after a partial write failure).

        Returns 0 if the table doesn't exist or is empty.
        """
        try:
            row = self.spark.sql(
                f"SELECT MAX(general_ledger_fact_id) AS max_pk FROM {fact_full_name}"
            ).first()
            return int(row["max_pk"]) if row and row["max_pk"] is not None else 0
        except Exception:
            return 0

    def reconcile_pk(self, fact_full_name: str) -> int:
        """
        Return the safe PK offset to use for the next batch, choosing the
        greater of ``last_fact_pk`` from state and the actual MAX PK in the table.

        This guards against a scenario where a batch wrote rows but the
        subsequent ``update()`` call failed — in that case the table has more
        rows than the state records, and we must not re-use those PKs.
        """
        state_pk  = self.load()["last_fact_pk"]
        actual_pk = self.get_max_pk_from_fact(fact_full_name)
        safe_pk   = max(state_pk, actual_pk)

        if safe_pk != state_pk:
            print(
                f"[StateManager] WARNING: state pk={state_pk:,} < actual max pk={actual_pk:,}. "
                f"Using actual max ({safe_pk:,}) as offset to avoid PK collisions."
            )
        return safe_pk
