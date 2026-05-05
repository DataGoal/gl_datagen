"""
src/orchestrator.py
--------------------
Main orchestration layer.

Generation order
----------------
1. All FK-referenced dimension tables (no dependencies)
2. general_ledger_fact (references all dims above)
3. CIS_fact, consolidated_balance_sheet_fact
4. Hierarchy and reference tables (no FK constraints on fact)
5. retail_global_store_profile_v

Usage (from Databricks notebook)
---------------------------------
    import sys
    sys.path.insert(0, "/Workspace/Repos/<user>/gl_datagen")
    from src.orchestrator import DataGenOrchestrator

    orch = DataGenOrchestrator(
        schema_path="configs/schema.yaml",
        volumes_path="configs/data_volumes.yaml",
    )
    orch.run()

Override volumes for a specific run without editing YAML
---------------------------------------------------------
    orch.run(overrides={"general_ledger_fact": {"rows": 25_000_000_000, "partitions": 1000}})
"""
from __future__ import annotations

import time
from typing import Dict, Any, Optional

import yaml
from pyspark.sql import SparkSession

from utils.datagen_helpers import FISCAL_MAX_PERIODS
from utils.schema_parser import SchemaParser
from utils.spark_utils import configure_spark, ensure_catalog_schema, write_table

from src.generators.dimension_generators import (
    DIVISION_TEXT_MAX_ROWS,
    # gen_accounting_document_type,
    gen_calendar_fiscal_period_v,
    gen_profit_center,
    # gen_division_text,
    gen_version_forecast_mapping,
    # gen_functional_area,
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
    # gen_CIS_fact,
    # gen_consolidated_balance_sheet_fact,
)
from src.generators.hierarchy_generators import (
    # gen_atscale_geo_security,
    # gen_consolidation_functional_area_hierarchy,
    #gen_consolidation_segment_hierarchy_dim,
    gen_segment_cost_center_hierarchy_dim_v,
    gen_segment_profit_center_hierarchy,
    gen_DisChannel_cost_center_hierarchy_dim_v,
    gen_DisChannel_profit_center_hierarchy,
    # gen_PartDisChannel_profit_center_hierarchy,
    gen_division_text_dim_v,
    # gen_gl_account_hierarchy,
    # gen_management_gl_account_hierarchy,
    gen_gl_account_zfsm_measures_hierarchy_dim,
    gen_finance_foreign_currency_exchange_rate,
    # gen_retail_global_store_profile_v,
)


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

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(
        self,
        tables: Optional[list] = None,
        overrides: Optional[Dict[str, Dict]] = None,
    ) -> None:
        """
        Generate and write all (or selected) tables.

        Parameters
        ----------
        tables    : Optional list of table names to generate. If None, all tables.
        overrides : Dict of {table_name: {rows: ..., partitions: ...}} to override
                    volume settings without editing YAML.
        """
        overrides = overrides or {}

        # Apply overrides
        for tbl, vals in overrides.items():
            if tbl not in self._volumes_raw:
                self._volumes_raw[tbl] = {}
            self._volumes_raw[tbl].update(vals)

        # Configure Spark
        gl_cfg = self._vol("general_ledger_fact")
        # configure_spark(self.spark, gl_cfg.get("shuffle_partitions", 400))

        # Ensure catalog + schema exist
        # ensure_catalog_schema(self.spark, self.catalog, self.db_schema)

        # Build the dim_rows lookup (used by fact generators for FK ranges)
        dim_rows = self._build_dim_rows_map()

        # Ordered generation plan
        plan = self._build_plan(dim_rows, tables)

        total_start = time.time()
        for step in plan:
            tbl_name = step["table"]
            t0 = time.time()
            print(f"\n[Orchestrator] Generating: {tbl_name} ({step['rows']:,} rows, {step['partitions']} partitions)")

            df = step["fn"]()
            write_table(df, self.schema.full_name(tbl_name), write_mode=self.write_mode)

            elapsed = time.time() - t0
            print(f"[Orchestrator] ✓ {tbl_name} done in {elapsed:.1f}s")

        total = time.time() - total_start
        print(f"\n[Orchestrator] ✅ All tables generated in {total:.1f}s")

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
        full = self.schema.full_name
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
            ("CIS_fact", "gl_account_id"),
            ("consolidated_balance_sheet_fact", "consolidated_balance_sheet_fact_id"),
            ("profit_center", "profit_center_id"),
            ("company_code", "company_id"),
            ("functional_area", "functional_area_id"),
            ("division_text", "division_id"),
            ("finance_product_dim_v", "product_id"),
            ("finance_customer_dim_v", "finance_customer_id"),
            ("copa_attribution_dim", "copa_attribution_id"),
            ("accounting_document_type", "accounting_document_type_id"),
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
                ("accounting_document_type_id", "accounting_document_type", "accounting_document_type_id"),
                ("fiscal_year_period_nbr",       "calendar_fiscal_period_v", "fiscal_year_period_nbr"),
                ("profit_center_id",             "profit_center",            "profit_center_id"),
                ("division_id",                  "division_text",            "division_id"),
                ("version_forecast_mapping_id",  "version_forecast_mapping", "version_forecast_mapping_id"),
                ("functional_area_id",           "functional_area",          "functional_area_id"),
                ("product_id",                   "finance_product_dim_v",    "product_id"),
                ("customer_id",                  "finance_customer_dim_v",   "finance_customer_id"),
                ("company_id",                   "company_code",             "company_id"),
                ("copa_attribution_id",          "copa_attribution_dim",     "copa_attribution_id"),
                ("cost_center_nbr",              "cost_center_dim_v",        "cost_center_nbr"),
                ("geo_wholesale_value_business_id", "geo_wholesale_value_business_dim", "geo_wholesale_value_business_id"),
                ("geo_marketplace_channel_id",   "geo_marketplace_channel_dim", "geo_marketplace_channel_id"),
                ("gl_account_nbr",               "gl_account_dim",           "gl_account_nbr"),
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

        # ---- Natural-key joinability on the secondary facts ------------------
        results["natural_key_integrity"] = {}
        nk_checks = [
            ("CIS_fact", "profit_center_nbr",    "profit_center",            "profit_center_nbr"),
            ("CIS_fact", "functional_area_cd",   "functional_area",          "functional_area_cd"),
            ("CIS_fact", "division_nbr",         "division_text",            "division_nbr"),
            ("CIS_fact", "fiscal_year_period_nbr", "calendar_fiscal_period_v", "fiscal_year_period_nbr"),
            ("consolidated_balance_sheet_fact", "profit_center_nbr",    "profit_center",   "profit_center_nbr"),
            ("consolidated_balance_sheet_fact", "functional_area_cd",   "functional_area", "functional_area_cd"),
            ("consolidated_balance_sheet_fact", "division_nbr",         "division_text",   "division_nbr"),
            ("consolidated_balance_sheet_fact", "fiscal_year_period_nbr","calendar_fiscal_period_v", "fiscal_year_period_nbr"),
        ]
        for fact_tbl, fact_col, ref_tbl, ref_col in nk_checks:
            if not (_exists(fact_tbl) and _exists(ref_tbl)):
                continue
            row = spark.sql(
                f"""
                SELECT
                  COUNT(*) AS rows,
                  SUM(CASE WHEN d.{ref_col} IS NULL AND f.{fact_col} IS NOT NULL THEN 1 ELSE 0 END) AS orphans
                FROM (SELECT * FROM {_q(fact_tbl)} LIMIT {int(sample_rows)}) f
                LEFT JOIN {_q(ref_tbl)} d
                  ON f.{fact_col} = d.{ref_col}
                """
            ).first()
            results["natural_key_integrity"][f"{fact_tbl}.{fact_col}->{ref_tbl}.{ref_col}"] = {
                "rows": row["rows"], "orphans": row["orphans"], "ok": (row["orphans"] or 0) == 0,
            }

        self._print_integrity_report(results)
        return results

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

        print("\n  Natural-key integrity (CIS_fact / consolidated_balance_sheet_fact)")
        for label, info in results.get("natural_key_integrity", {}).items():
            mark = "OK" if info["ok"] else "FAIL"
            print(f"    [{mark}] {label:<80} rows={info['rows']:,}  orphans={info['orphans']}")

        print("=" * 78)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _vol(self, table: str) -> Dict[str, Any]:
        """Return volume config for a table with defaults."""
        cfg = self._volumes_raw.get(table, {})
        return {
            "rows": cfg.get("rows", 1000),
            "partitions": cfg.get("partitions", 4),
            "shuffle_partitions": cfg.get("shuffle_partitions",
                                          self._volumes_raw.get("general_ledger_fact", {}).get("shuffle_partitions", 400)),
        }

    def _build_dim_rows_map(self) -> Dict[str, int]:
        """
        Build a mapping of ``{table_name: row_count}`` for all dimensions whose
        PK domain is sampled by the fact tables. Used to set FK column ranges.

        ``calendar_fiscal_period_v`` is capped at :data:`FISCAL_MAX_PERIODS`
        because the calendar dim itself only materialises that many distinct
        ``fiscal_year_period_nbr`` values regardless of the configured row count.

        ``division_text`` is capped at :data:`DIVISION_TEXT_MAX_ROWS` (20)
        because ``gen_division_text`` uses a hardcoded list of exactly 20
        divisions; any YAML row count above 20 would cause ``division_id`` FK
        values in the fact table to reference non-existent dim rows.
        """
        cal_cfg = self._vol("calendar_fiscal_period_v")["rows"]
        cal_actual = min(int(cal_cfg), FISCAL_MAX_PERIODS)

        div_cfg = self._vol("division_text")["rows"]
        div_actual = min(int(div_cfg), DIVISION_TEXT_MAX_ROWS)

        return {
            "accounting_document_type": self._vol("accounting_document_type")["rows"],
            "calendar_fiscal_period_v": cal_actual,
            "profit_center": self._vol("profit_center")["rows"],
            "division_text": div_actual,
            "version_forecast_mapping": self._vol("version_forecast_mapping")["rows"],
            "functional_area": self._vol("functional_area")["rows"],
            "finance_product_dim_v": self._vol("finance_product_dim_v")["rows"],
            "finance_customer_dim_v": self._vol("finance_customer_dim_v")["rows"],
            "company_code": self._vol("company_code")["rows"],
            "copa_attribution_dim": self._vol("copa_attribution_dim")["rows"],
            "cost_center_dim_v": self._vol("cost_center_dim_v")["rows"],
            "geo_wholesale_value_business_dim": self._vol("geo_wholesale_value_business_dim")["rows"],
            "geo_marketplace_channel_dim": self._vol("geo_marketplace_channel_dim")["rows"],
            "gl_account_dim": self._vol("gl_account_dim")["rows"],
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
        # Includes every dim referenced (formally OR via natural-key columns)
        # by any fact table, so that running the orchestrator with a
        # fact-only filter still produces a referentially-consistent dataset.
        dim_generators = [
            # ("accounting_document_type",
            #  lambda r, p: gen_accounting_document_type(sp, r, p)),
            ("calendar_fiscal_period_v",
             lambda r, p: gen_calendar_fiscal_period_v(sp, r, p)),
            ("profit_center",
             lambda r, p: gen_profit_center(sp, r, p)),
            # ("division_text",
            #  lambda r, p: gen_division_text(sp, r, p)),
            ("version_forecast_mapping",
             lambda r, p: gen_version_forecast_mapping(sp, r, p)),
            # ("functional_area",
            #  lambda r, p: gen_functional_area(sp, r, p)),
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
            # Referenced by general_ledger_fact as zfsm_measure_id and the
            # currency-exchange-rate ids. Materialised here so they exist
            # whenever facts are generated.
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

        # ---- Phase 2: Fact tables ---------------------------------------------
        if include("general_ledger_fact"):
            r, p = cfg("general_ledger_fact")
            steps.append({
                "table": "general_ledger_fact", "rows": r, "partitions": p,
                "fn": lambda _r=r, _p=p: gen_general_ledger_fact(sp, _r, _p, dim_rows),
            })

        # if include("CIS_fact"):
        #     r, p = cfg("CIS_fact")
        #     steps.append({
        #         "table": "CIS_fact", "rows": r, "partitions": p,
        #         "fn": lambda _r=r, _p=p: gen_CIS_fact(sp, _r, _p, dim_rows),
        #     })

        # if include("consolidated_balance_sheet_fact"):
        #     r, p = cfg("consolidated_balance_sheet_fact")
        #     steps.append({
        #         "table": "consolidated_balance_sheet_fact", "rows": r, "partitions": p,
        #         "fn": lambda _r=r, _p=p: gen_consolidated_balance_sheet_fact(sp, _r, _p, dim_rows),
        #     })

        # ---- Phase 3: Hierarchy & reference tables ----------------------------
        hierarchy_generators = [
            # ("atscale_geo_security", lambda r, p: gen_atscale_geo_security(sp, r, p)),
            # ("consolidation_functional_area_hierarchy",
            #  lambda r, p: gen_consolidation_functional_area_hierarchy(sp, r, p)),
            # ("consolidation_segment_hierarchy_dim",
            #  lambda r, p: gen_consolidation_segment_hierarchy_dim(sp, r, p)),
            ("segment_cost_center_hierarchy_dim_v",
             lambda r, p: gen_segment_cost_center_hierarchy_dim_v(sp, r, p)),
            ("segment_profit_center_hierarchy",
             lambda r, p: gen_segment_profit_center_hierarchy(sp, r, p)),
            ("DisChannel_cost_center_hierarchy_dim_v",
             lambda r, p: gen_DisChannel_cost_center_hierarchy_dim_v(sp, r, p)),
            ("DisChannel_profit_center_hierarchy",
             lambda r, p: gen_DisChannel_profit_center_hierarchy(sp, r, p)),
            # ("PartDisChannel_profit_center_hierarchy",
            #  lambda r, p: gen_PartDisChannel_profit_center_hierarchy(sp, r, p)),
            ("division_text_dim_v",
             lambda r, p: gen_division_text_dim_v(sp, r, p)),
            # ("gl_account_hierarchy",
            #  lambda r, p: gen_gl_account_hierarchy(sp, r, p)),
            # ("management_gl_account_hierarchy",
            #  lambda r, p: gen_management_gl_account_hierarchy(sp, r, p)),
            # ("retail_global_store_profile_v",
            #  lambda r, p: gen_retail_global_store_profile_v(sp, r, p)),
        ]

        for tbl, fn in hierarchy_generators:
            if include(tbl):
                r, p = cfg(tbl)
                steps.append({"table": tbl, "rows": r, "partitions": p,
                               "fn": (lambda _r=r, _p=p, _fn=fn: _fn(_r, _p))})

        return steps
