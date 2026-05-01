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

from utils.schema_parser import SchemaParser
from utils.spark_utils import configure_spark, ensure_catalog_schema, write_table

from src.generators.dimension_generators import (
    gen_accounting_document_type,
    gen_calendar_fiscal_period_v,
    gen_profit_center,
    gen_division_text,
    gen_version_forecast_mapping,
    gen_functional_area,
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
    gen_CIS_fact,
    gen_consolidated_balance_sheet_fact,
)
from src.generators.hierarchy_generators import (
    gen_atscale_geo_security,
    gen_consolidation_functional_area_hierarchy,
    gen_consolidation_segment_hierarchy_dim,
    gen_segment_cost_center_hierarchy_dim_v,
    gen_segment_profit_center_hierarchy,
    gen_DisChannel_cost_center_hierarchy_dim_v,
    gen_DisChannel_profit_center_hierarchy,
    gen_PartDisChannel_profit_center_hierarchy,
    gen_division_text_dim_v,
    gen_gl_account_hierarchy,
    gen_management_gl_account_hierarchy,
    gen_gl_account_zfsm_measures_hierarchy_dim,
    gen_finance_foreign_currency_exchange_rate,
    gen_retail_global_store_profile_v,
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
        Build a mapping of {table_name: row_count} for all dimensions
        referenced by the fact table. Used to set FK column ranges.
        """
        return {
            "accounting_document_type": self._vol("accounting_document_type")["rows"],
            "calendar_fiscal_period_v": self._vol("calendar_fiscal_period_v")["rows"],
            "profit_center": self._vol("profit_center")["rows"],
            "division_text": self._vol("division_text")["rows"],
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
        dim_generators = [
            ("accounting_document_type",
             lambda r, p: gen_accounting_document_type(sp, r, p)),
            ("calendar_fiscal_period_v",
             lambda r, p: gen_calendar_fiscal_period_v(sp, r, p)),
            ("profit_center",
             lambda r, p: gen_profit_center(sp, r, p)),
            ("division_text",
             lambda r, p: gen_division_text(sp, r, p)),
            ("version_forecast_mapping",
             lambda r, p: gen_version_forecast_mapping(sp, r, p)),
            ("functional_area",
             lambda r, p: gen_functional_area(sp, r, p)),
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

        if include("CIS_fact"):
            r, p = cfg("CIS_fact")
            steps.append({
                "table": "CIS_fact", "rows": r, "partitions": p,
                "fn": lambda _r=r, _p=p: gen_CIS_fact(sp, _r, _p, dim_rows),
            })

        if include("consolidated_balance_sheet_fact"):
            r, p = cfg("consolidated_balance_sheet_fact")
            steps.append({
                "table": "consolidated_balance_sheet_fact", "rows": r, "partitions": p,
                "fn": lambda _r=r, _p=p: gen_consolidated_balance_sheet_fact(sp, _r, _p),
            })

        # ---- Phase 3: Hierarchy & reference tables ----------------------------
        hierarchy_generators = [
            ("atscale_geo_security", lambda r, p: gen_atscale_geo_security(sp, r, p)),
            ("consolidation_functional_area_hierarchy",
             lambda r, p: gen_consolidation_functional_area_hierarchy(sp, r, p)),
            ("consolidation_segment_hierarchy_dim",
             lambda r, p: gen_consolidation_segment_hierarchy_dim(sp, r, p)),
            ("segment_cost_center_hierarchy_dim_v",
             lambda r, p: gen_segment_cost_center_hierarchy_dim_v(sp, r, p)),
            ("segment_profit_center_hierarchy",
             lambda r, p: gen_segment_profit_center_hierarchy(sp, r, p)),
            ("DisChannel_cost_center_hierarchy_dim_v",
             lambda r, p: gen_DisChannel_cost_center_hierarchy_dim_v(sp, r, p)),
            ("DisChannel_profit_center_hierarchy",
             lambda r, p: gen_DisChannel_profit_center_hierarchy(sp, r, p)),
            ("PartDisChannel_profit_center_hierarchy",
             lambda r, p: gen_PartDisChannel_profit_center_hierarchy(sp, r, p)),
            ("division_text_dim_v",
             lambda r, p: gen_division_text_dim_v(sp, r, p)),
            ("gl_account_hierarchy",
             lambda r, p: gen_gl_account_hierarchy(sp, r, p)),
            ("management_gl_account_hierarchy",
             lambda r, p: gen_management_gl_account_hierarchy(sp, r, p)),
            ("gl_account_zfsm_measures_hierarchy_dim",
             lambda r, p: gen_gl_account_zfsm_measures_hierarchy_dim(sp, r, p)),
            ("finance_foreign_currency_exchange_rate",
             lambda r, p: gen_finance_foreign_currency_exchange_rate(sp, r, p)),
            ("retail_global_store_profile_v",
             lambda r, p: gen_retail_global_store_profile_v(sp, r, p)),
        ]

        for tbl, fn in hierarchy_generators:
            if include(tbl):
                r, p = cfg(tbl)
                steps.append({"table": tbl, "rows": r, "partitions": p,
                               "fn": (lambda _r=r, _p=p, _fn=fn: _fn(_r, _p))})

        return steps
