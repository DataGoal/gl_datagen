"""
src/orchestrator.py
--------------------
Main orchestration layer.

Generation order (initial load)
---------------------------------
1. All FK-referenced dimension tables (no dependencies on each other)
2. general_ledger_fact (references all dims above)
3. Hierarchy and reference tables (no FK constraints from fact)

Incremental load
----------------
Only ``general_ledger_fact`` is generated and appended.  Dimension tables are
left untouched.  PK continuity and FK validity are guaranteed through the
``IncrementalStateManager`` which freezes the ``dim_rows`` map at initial-load
time and tracks the highest ``general_ledger_fact_id`` written so far.

Usage — initial load (Databricks notebook)
-------------------------------------------
    from src.orchestrator import DataGenOrchestrator

    orch = DataGenOrchestrator(
        schema_path="configs/schema.yaml",
        volumes_path="configs/data_volumes.yaml",
    )
    orch.run(mode="initial")

Usage — incremental load
--------------------------
    orch.run(
        mode="incremental",
        overrides={"general_ledger_fact": {"rows": 500_000_000, "partitions": 200}},
    )

Override volumes without editing YAML
--------------------------------------
    orch.run(
        mode="initial",
        overrides={"general_ledger_fact": {"rows": 25_000_000_000, "partitions": 1000}},
    )
"""
from __future__ import annotations

import time
from typing import Dict, Any, Optional

import yaml
from pyspark.sql import SparkSession

from utils.datagen_helpers import FISCAL_MAX_PERIODS
from utils.schema_parser import SchemaParser
from utils.spark_utils import configure_spark, ensure_catalog_schema, write_table
from utils.state_manager import IncrementalStateManager

from src.generators.dimension_generators import (
    DIVISION_TEXT_MAX_ROWS,
    gen_calendar_fiscal_period_v,
    gen_profit_center,
    gen_version_forecast_mapping,
    gen_finance_product_dim_v,
    gen_finance_customer_dim_v,
    gen_company_code,
    gen_copa_attribution_dim,
    gen_cost_center_dim_v,
    gen_geo_wholesale_value_business_dim,
    gen_geo_marketplace_channel_dim,
    gen_gl_account_dim,
)
from src.generators.fact_generators import (
    gen_general_ledger_fact,
)
from src.generators.hierarchy_generators import (
    gen_segment_cost_center_hierarchy_dim_v,
    gen_segment_profit_center_hierarchy,
    gen_DisChannel_cost_center_hierarchy_dim_v,
    gen_DisChannel_profit_center_hierarchy,
    gen_division_text_dim_v,
    gen_gl_account_zfsm_measures_hierarchy_dim,
    gen_finance_foreign_currency_exchange_rate,
)

# Seed used for the initial load and as the base for incremental batch seeds.
_BASE_SEED: int = 42


class DataGenOrchestrator:
    """
    Reads configuration, wires up generators, and writes tables to Delta.

    Parameters
    ----------
    schema_path  : Path to schema.yaml (relative to repo root or absolute).
    volumes_path : Path to data_volumes.yaml.
    spark        : Optional SparkSession (defaults to getOrCreate()).
    """

    def __init__(
        self,
        schema_path: str = "configs/schema.yaml",
        volumes_path: str = "configs/data_volumes.yaml",
        spark: Optional[SparkSession] = None,
    ):
        self.schema = SchemaParser(schema_path)

        with open(volumes_path, "r") as fh:
            self._volumes_raw: Dict[str, Any] = yaml.safe_load(fh)

        self.spark = spark or SparkSession.builder.getOrCreate()
        self._env = self._volumes_raw.get("environment", {})
        self.catalog = self._env.get("catalog", self.schema.catalog)
        self.db_schema = self._env.get("schema", self.schema.schema)
        self.write_mode = self._env.get("write_mode", "overwrite")

        # Incremental configuration block from data_volumes.yaml
        self._inc_cfg: Dict[str, Any] = self._volumes_raw.get("incremental", {})

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(
        self,
        tables: Optional[list] = None,
        overrides: Optional[Dict[str, Dict]] = None,
        mode: str = "initial",
    ) -> None:
        """
        Generate and write tables to Delta.

        Parameters
        ----------
        tables    : Optional list of table names to generate (initial mode only).
                    If None, all tables are generated.
        overrides : Dict of ``{table_name: {rows: ..., partitions: ...}}`` to
                    override volume settings without editing YAML.
        mode      : ``"initial"``     — generate all (or selected) tables from
                                        scratch; saves incremental state on completion.
                    ``"incremental"`` — append a new batch to ``general_ledger_fact``
                                        only; dimensions are never touched.
        """
        if mode == "incremental":
            self._run_incremental(overrides or {})
        else:
            self._run_initial(tables, overrides or {})

    def verify_integrity(self, sample_rows: int = 1_000_000) -> Dict[str, Any]:
        """
        Run lightweight referential-integrity and PK-uniqueness checks against
        the generated tables. Designed to run quickly on huge fact tables by
        sampling a fixed number of rows from the central fact.

        Returns a dict with per-check counts so callers can assert on them
        from a notebook. Prints a human-readable summary.

        Checks performed
        ----------------
        * PK uniqueness: ``general_ledger_fact_id``, dimension PKs.
        * FK integrity: each declared FK on ``general_ledger_fact`` resolves to
          a row in its referenced dim.
        * Natural-key linkage: ``CIS_fact``/``consolidated_balance_sheet_fact``
          ``profit_center_nbr`` and ``functional_area_cd`` resolve to rows in
          their respective dim tables.
        """
        spark = self.spark
        results: Dict[str, Any] = {}

        def _q(table: str) -> str:
            return f"`{self.catalog}`.`{self.db_schema}`.`{table}`"

        def _exists(table: str) -> bool:
            try:
                spark.table(_q(table))
                return True
            except Exception:
                return False

        # ---- PK uniqueness on the central fact and key dims ------------------
        pk_checks = [
            ("general_ledger_fact", "general_ledger_fact_id"),
            ("profit_center", "profit_center_id"),
            ("company_code", "company_id"),
            ("finance_product_dim_v", "product_id"),
            ("finance_customer_dim_v", "finance_customer_id"),
            ("copa_attribution_dim", "copa_attribution_id"),
            ("calendar_fiscal_period_v", "fiscal_year_period_nbr"),
            ("cost_center_dim_v", "cost_center_nbr"),
            ("gl_account_dim", "gl_account_nbr"),
        ]
        results["pk_uniqueness"] = {}
        for tbl, pk in pk_checks:
            if not _exists(tbl):
                continue
            row = spark.sql(
                f"SELECT COUNT(*) AS total, COUNT(DISTINCT `{pk}`) AS distinct "
                f"FROM {_q(tbl)}"
            ).first()
            ok = row["total"] == row["distinct"] and row["total"] > 0
            results["pk_uniqueness"][tbl] = {
                "pk": pk, "rows": row["total"], "distinct": row["distinct"], "ok": ok,
            }

        # ---- FK integrity on a sample of general_ledger_fact -----------------
        results["fk_integrity"] = {}
        if _exists("general_ledger_fact"):
            sample_view = "__gl_fact_sample"
            spark.sql(
                f"CREATE OR REPLACE TEMP VIEW {sample_view} AS "
                f"SELECT * FROM {_q('general_ledger_fact')} LIMIT {int(sample_rows)}"
            )

            fk_checks = [
                ("fiscal_year_period_nbr",          "calendar_fiscal_period_v",          "fiscal_year_period_nbr"),
                ("profit_center_id",                "profit_center",                     "profit_center_id"),
                ("version_forecast_mapping_id",     "version_forecast_mapping",          "version_forecast_mapping_id"),
                ("product_id",                      "finance_product_dim_v",             "product_id"),
                ("customer_id",                     "finance_customer_dim_v",            "finance_customer_id"),
                ("company_id",                      "company_code",                      "company_id"),
                ("copa_attribution_id",             "copa_attribution_dim",              "copa_attribution_id"),
                ("cost_center_nbr",                 "cost_center_dim_v",                 "cost_center_nbr"),
                ("geo_wholesale_value_business_id", "geo_wholesale_value_business_dim",  "geo_wholesale_value_business_id"),
                ("geo_marketplace_channel_id",      "geo_marketplace_channel_dim",       "geo_marketplace_channel_id"),
                ("gl_account_nbr",                  "gl_account_dim",                    "gl_account_nbr"),
            ]
            for fk_col, ref_tbl, ref_col in fk_checks:
                if not _exists(ref_tbl):
                    continue
                row = spark.sql(
                    f"""
                    SELECT
                      COUNT(*)               AS sampled,
                      COUNT(DISTINCT f.{fk_col}) AS fk_distinct,
                      SUM(CASE WHEN d.{ref_col} IS NULL THEN 1 ELSE 0 END) AS orphans
                    FROM {sample_view} f
                    LEFT JOIN {_q(ref_tbl)} d
                      ON f.{fk_col} = d.{ref_col}
                    """
                ).first()
                results["fk_integrity"][f"{fk_col}->{ref_tbl}.{ref_col}"] = {
                    "sampled": row["sampled"],
                    "fk_distinct_in_sample": row["fk_distinct"],
                    "orphans": row["orphans"],
                    "ok": (row["orphans"] or 0) == 0,
                }

        results["natural_key_integrity"] = {}

        self._print_integrity_report(results)
        return results

    def get_incremental_state(self) -> Optional[Dict[str, Any]]:
        """
        Return the current incremental state dict, or None if not initialized.

        Useful for monitoring / debugging from a notebook.
        """
        mgr = self._make_state_manager()
        if mgr.exists():
            return mgr.load()
        return None

    # ------------------------------------------------------------------
    # Private — mode implementations
    # ------------------------------------------------------------------

    def _run_initial(
        self,
        tables: Optional[list],
        overrides: Dict[str, Any],
    ) -> None:
        """Generate all (or selected) tables and save incremental state."""

        # Apply YAML overrides
        for tbl, vals in overrides.items():
            if tbl not in self._volumes_raw:
                self._volumes_raw[tbl] = {}
            self._volumes_raw[tbl].update(vals)

        dim_rows = self._build_dim_rows_map()
        plan = self._build_plan(dim_rows, tables)

        total_start = time.time()
        for step in plan:
            tbl_name = step["table"]
            t0 = time.time()
            print(f"\n[Orchestrator] Generating: {tbl_name} "
                  f"({step['rows']:,} rows, {step['partitions']} partitions)")
            df = step["fn"]()
            write_table(df, self.schema.full_name(tbl_name), write_mode=self.write_mode)
            elapsed = time.time() - t0
            print(f"[Orchestrator] ✓ {tbl_name} done in {elapsed:.1f}s")

        total = time.time() - total_start
        print(f"\n[Orchestrator] ✅ All tables generated in {total:.1f}s")

        # Persist state so incremental runs know the PK ceiling and dim_rows.
        # Only save state when the fact table was (or could have been) written.
        fact_included = tables is None or "general_ledger_fact" in (tables or [])
        if fact_included:
            fact_rows = self._vol("general_ledger_fact")["rows"]
            mgr = self._make_state_manager()
            mgr.initialize(dim_rows, fact_rows, seed=_BASE_SEED)

    def _run_incremental(self, overrides: Dict[str, Any]) -> None:
        """
        Append a new batch of rows to ``general_ledger_fact``.

        FK bounds come from the frozen ``dim_rows`` snapshot in state.
        PK values start immediately after the last written ``general_ledger_fact_id``.
        Each batch uses a unique seed (``base_seed + batch_number``) for
        reproducible but non-overlapping data distributions.
        """
        mgr = self._make_state_manager()

        if not mgr.exists():
            raise RuntimeError(
                "Incremental state not found. Run mode='initial' first to generate "
                "all dimension tables and initialize the state tracker."
            )

        # Retrieve frozen state
        state = mgr.load()
        dim_rows   = state["dim_rows"]
        base_seed  = state["initial_seed"]
        batch_num  = state["batch_count"] + 1  # next batch number (1-indexed)

        # Reconcile PK offset: take the max of stored value and actual table max.
        fact_full  = self.schema.full_name("general_ledger_fact")
        pk_offset  = mgr.reconcile_pk(fact_full)

        # Batch-specific seed: base + batch_num for deterministic-but-distinct batches
        batch_seed = base_seed + batch_num

        # Resolve batch size from YAML incremental block, then overrides
        inc_rows   = self._inc_cfg.get("batch_rows", 500_000_000)
        inc_parts  = self._inc_cfg.get("batch_partitions", 200)

        # Apply caller overrides (e.g. from notebook widget)
        fact_ov = overrides.get("general_ledger_fact", {})
        if "rows" in fact_ov:
            inc_rows = int(fact_ov["rows"])
        if "partitions" in fact_ov:
            inc_parts = int(fact_ov["partitions"])

        print(
            f"\n[Orchestrator] Incremental batch {batch_num}:\n"
            f"  pk_offset   = {pk_offset:,}\n"
            f"  batch_rows  = {inc_rows:,}\n"
            f"  partitions  = {inc_parts}\n"
            f"  seed        = {batch_seed}\n"
            f"  target PKs  = [{pk_offset + 1:,} .. {pk_offset + inc_rows:,}]"
        )

        t0 = time.time()
        df = gen_general_ledger_fact(
            self.spark, inc_rows, inc_parts, dim_rows,
            pk_offset=pk_offset,
            seed=batch_seed,
        )
        write_table(df, fact_full, write_mode="append")
        elapsed = time.time() - t0

        # Update persistent state
        updated = mgr.update(inc_rows)
        print(
            f"[Orchestrator] ✅ Incremental batch {batch_num} done in {elapsed:.1f}s  "
            f"| total fact rows = {updated['total_fact_rows']:,}"
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _make_state_manager(self) -> IncrementalStateManager:
        """Construct a state manager pointed at the configured state table."""
        state_tbl_name = self._inc_cfg.get("state_table", "_datagen_incremental_state")
        state_full = self.schema.full_name(state_tbl_name)
        return IncrementalStateManager(self.spark, state_full)

    def _vol(self, table: str) -> Dict[str, Any]:
        """Return volume config for a table with defaults."""
        cfg = self._volumes_raw.get(table, {})
        return {
            "rows": cfg.get("rows", 1000),
            "partitions": cfg.get("partitions", 4),
            "shuffle_partitions": cfg.get(
                "shuffle_partitions",
                self._volumes_raw.get("general_ledger_fact", {}).get("shuffle_partitions", 400),
            ),
        }

    def _build_dim_rows_map(self) -> Dict[str, int]:
        """
        Build a mapping of ``{table_name: row_count}`` for all dimensions whose
        PK domain is sampled by the fact tables.  Used to set FK column ranges.

        ``calendar_fiscal_period_v`` is capped at :data:`FISCAL_MAX_PERIODS`
        because the calendar dim itself only materialises that many distinct
        ``fiscal_year_period_nbr`` values regardless of the configured row count.

        ``division_text`` is capped at :data:`DIVISION_TEXT_MAX_ROWS` (20)
        because ``gen_division_text`` uses a hardcoded list of exactly 20
        divisions; any YAML row count above 20 would cause ``division_id`` FK
        values in the fact table to reference non-existent dim rows.
        """
        cal_cfg    = self._vol("calendar_fiscal_period_v")["rows"]
        cal_actual = min(int(cal_cfg), FISCAL_MAX_PERIODS)

        return {
            "calendar_fiscal_period_v":              cal_actual,
            "profit_center":                         self._vol("profit_center")["rows"],
            "version_forecast_mapping":              self._vol("version_forecast_mapping")["rows"],
            "finance_product_dim_v":                 self._vol("finance_product_dim_v")["rows"],
            "finance_customer_dim_v":                self._vol("finance_customer_dim_v")["rows"],
            "company_code":                          self._vol("company_code")["rows"],
            "copa_attribution_dim":                  self._vol("copa_attribution_dim")["rows"],
            "cost_center_dim_v":                     self._vol("cost_center_dim_v")["rows"],
            "geo_wholesale_value_business_dim":       self._vol("geo_wholesale_value_business_dim")["rows"],
            "geo_marketplace_channel_dim":           self._vol("geo_marketplace_channel_dim")["rows"],
            "gl_account_dim":                        self._vol("gl_account_dim")["rows"],
            "gl_account_zfsm_measures_hierarchy_dim": self._vol("gl_account_zfsm_measures_hierarchy_dim")["rows"],
            "finance_foreign_currency_exchange_rate": self._vol("finance_foreign_currency_exchange_rate")["rows"],
        }

    def _build_plan(self, dim_rows: Dict[str, int], only: Optional[list]) -> list:
        """Return ordered list of generation steps."""
        sp = self.spark

        def cfg(t):
            v = self._vol(t)
            return v["rows"], v["partitions"]

        def include(t):
            return only is None or t in only

        steps = []

        # ---- Phase 1: Dimension tables ----------------------------------------
        dim_generators = [
            ("calendar_fiscal_period_v",
             lambda r, p: gen_calendar_fiscal_period_v(sp, r, p)),
            ("profit_center",
             lambda r, p: gen_profit_center(sp, r, p)),
            ("version_forecast_mapping",
             lambda r, p: gen_version_forecast_mapping(sp, r, p)),
            ("finance_product_dim_v",
             lambda r, p: gen_finance_product_dim_v(sp, r, p)),
            ("finance_customer_dim_v",
             lambda r, p: gen_finance_customer_dim_v(sp, r, p)),
            ("company_code",
             lambda r, p: gen_company_code(sp, r, p)),
            ("copa_attribution_dim",
             lambda r, p: gen_copa_attribution_dim(sp, r, p)),
            ("cost_center_dim_v",
             lambda r, p: gen_cost_center_dim_v(sp, r, p)),
            ("geo_wholesale_value_business_dim",
             lambda r, p: gen_geo_wholesale_value_business_dim(sp, r, p)),
            ("geo_marketplace_channel_dim",
             lambda r, p: gen_geo_marketplace_channel_dim(sp, r, p)),
            ("gl_account_dim",
             lambda r, p: gen_gl_account_dim(sp, r, p)),
            ("gl_account_zfsm_measures_hierarchy_dim",
             lambda r, p: gen_gl_account_zfsm_measures_hierarchy_dim(sp, r, p)),
            ("finance_foreign_currency_exchange_rate",
             lambda r, p: gen_finance_foreign_currency_exchange_rate(sp, r, p)),
        ]

        for tbl, fn in dim_generators:
            if include(tbl):
                r, p = cfg(tbl)
                steps.append({"table": tbl, "rows": r, "partitions": p,
                               "fn": (lambda _r=r, _p=p, _fn=fn: _fn(_r, _p))})

        # ---- Phase 2: Fact tables (initial load only; seed=_BASE_SEED) --------
        if include("general_ledger_fact"):
            r, p = cfg("general_ledger_fact")
            steps.append({
                "table": "general_ledger_fact", "rows": r, "partitions": p,
                "fn": lambda _r=r, _p=p: gen_general_ledger_fact(
                    sp, _r, _p, dim_rows, pk_offset=0, seed=_BASE_SEED
                ),
            })

        # ---- Phase 3: Hierarchy & reference tables ----------------------------
        hierarchy_generators = [
            ("segment_cost_center_hierarchy_dim_v",
             lambda r, p: gen_segment_cost_center_hierarchy_dim_v(sp, r, p)),
            ("segment_profit_center_hierarchy",
             lambda r, p: gen_segment_profit_center_hierarchy(sp, r, p)),
            ("DisChannel_cost_center_hierarchy_dim_v",
             lambda r, p: gen_DisChannel_cost_center_hierarchy_dim_v(sp, r, p)),
            ("DisChannel_profit_center_hierarchy",
             lambda r, p: gen_DisChannel_profit_center_hierarchy(sp, r, p)),
            ("division_text_dim_v",
             lambda r, p: gen_division_text_dim_v(sp, r, p)),
        ]

        for tbl, fn in hierarchy_generators:
            if include(tbl):
                r, p = cfg(tbl)
                steps.append({"table": tbl, "rows": r, "partitions": p,
                               "fn": (lambda _r=r, _p=p, _fn=fn: _fn(_r, _p))})

        return steps

    @staticmethod
    def _print_integrity_report(results: Dict[str, Any]) -> None:
        print("\n" + "=" * 78)
        print("[Orchestrator] Integrity report")
        print("=" * 78)

        print("\n  PK uniqueness")
        for tbl, info in results.get("pk_uniqueness", {}).items():
            mark = "OK" if info["ok"] else "FAIL"
            print(f"    [{mark}] {tbl:<45} {info['pk']:<35} "
                  f"rows={info['rows']:>14,}  distinct={info['distinct']:>14,}")

        print("\n  FK integrity (general_ledger_fact sample)")
        for label, info in results.get("fk_integrity", {}).items():
            mark = "OK" if info["ok"] else "FAIL"
            print(f"    [{mark}] {label:<70} sampled={info['sampled']:,}  orphans={info['orphans']}")

        print("=" * 78)
