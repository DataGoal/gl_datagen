"""
src/generators/fact_generators.py
-----------------------------------
Generators for all fact tables.

FK integrity strategy
---------------------
All dimension bigint PKs are generated as sequential ranges [1..dim_rows].
FK columns in facts use minValue=1, maxValue=dim_rows so every generated FK
maps to a valid dimension PK — no join, no broadcast, scales to 25B rows.

String PK dimensions (cost_center_dim_v -> CC_000001, gl_account_dim -> GL_000001)
are replicated with the same prefix/pad formula so string FKs are equally valid.

Calendar PKs are drawn from a single shared list (``fiscal_period_values``) used
by both the calendar dim and every fact's ``fiscal_year_period_nbr`` FK.

The non-FK dimensional columns on ``CIS_fact`` and
``consolidated_balance_sheet_fact`` (e.g. ``profit_center_nbr``,
``functional_area_cd``, ``division_nbr``) are also generated from the same
domains as their parent dimensions so downstream aggregated tables can join
on these natural keys without orphans.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from utils.datagen_helpers import (
    active_ind_col,
    date_col,
    decimal_amount,
    enum_col,
    fiscal_year_period,
    fk_bigint,
    fk_int,
    fk_string,
    make_generator,
    pk_bigint,
    pk_bigint_range,
    physical_source_col,
    template_string,
    user_id_col,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _division_nbr_values(dim_rows: dict) -> list:
    """
    Return the deterministic list of division_nbr strings produced by
    ``gen_division_text`` (``"01"`` ... ``"NN"`` for the first N divisions).
    Used by fact tables so their string division_nbr columns join cleanly to
    ``division_text``.
    """
    n = max(1, min(int(dim_rows.get("division_text", 20)), 99))
    return [f"{i:02d}" for i in range(1, n + 1)]


def _derive_fiscal_yr(gen, period_col: str = "fiscal_year_period_nbr",
                     out_col: str = "fiscal_yr"):
    """Derive ``fiscal_yr`` (YYYY) directly from ``fiscal_year_period_nbr``."""
    return gen.withColumn(
        out_col, T.IntegerType(),
        baseColumn=period_col,
        expr=f"cast({period_col} / 100 as int)",
    )


# ---------------------------------------------------------------------------
# general_ledger_fact  (central fact table)
# ---------------------------------------------------------------------------

def gen_general_ledger_fact(
    spark: SparkSession,
    rows: int,
    partitions: int,
    dim_rows: dict,          # {"table_name": row_count, ...}
    pk_offset: int = 0,      # 0 = initial load; >0 = incremental (last written PK)
    seed: int = 42,          # deterministic seed; increment per batch for distinct data
) -> DataFrame:
    """
    Generate the central general_ledger_fact table.

    Parameters
    ----------
    dim_rows  : dict mapping each referenced dimension table name to its row count.
                Used to bound FK column ranges for referential integrity.
                Example: {"profit_center": 5000, "company_code": 500, ...}
    pk_offset : For incremental loads, the highest ``general_ledger_fact_id`` already
                written.  New rows will receive PKs in ``[pk_offset+1, pk_offset+rows]``.
                Set to 0 (default) for the initial full load.
    seed      : Random seed passed to dbldatagen.  Use ``base_seed + batch_number``
                for incremental batches so each batch produces distinct but reproducible
                measure/FK distributions.
    """
    gen = make_generator(spark, "general_ledger_fact", rows, partitions, seed=seed)

    # ---- PK ------------------------------------------------------------------
    if pk_offset > 0:
        # Incremental load: assign PKs in the range (pk_offset, pk_offset + rows]
        gen = pk_bigint_range(
            gen, "general_ledger_fact_id",
            start=pk_offset + 1,
            end=pk_offset + rows,
        )
    else:
        # Initial load: standard sequential PKs [1..rows]
        gen = pk_bigint(gen, "general_ledger_fact_id")

    # ---- FK -> calendar_fiscal_period_v (int PK: YYYYPP values) -------------
    # Use the SAME first-N list the dim materialises to guarantee FK validity.
    gen = fiscal_year_period(
        gen, "fiscal_year_period_nbr",
        count=dim_rows.get("calendar_fiscal_period_v", 108),
    )

    # ---- FK -> profit_center (bigint PK) ------------------------------------
    gen = fk_bigint(gen, "profit_center_id", dim_rows.get("profit_center", 500))

    # ---- FK -> division_text (bigint PK) ------------------------------------
    gen = fk_bigint(gen, "division_id", dim_rows.get("division_text", 20))

    # ---- FK -> version_forecast_mapping (bigint PK) -------------------------
    gen = fk_bigint(gen, "version_forecast_mapping_id",
                    dim_rows.get("version_forecast_mapping", 30))

    # ---- FK -> functional_area (bigint PK) ----------------------------------
    gen = fk_bigint(gen, "functional_area_id", dim_rows.get("functional_area", 100))

    # ---- FK -> accounting_document_type (bigint PK) -------------------------
    gen = fk_bigint(gen, "accounting_document_type_id",
                    dim_rows.get("accounting_document_type", 50))

    # ---- FK -> finance_product_dim_v (bigint PK) ----------------------------
    gen = fk_bigint(gen, "product_id", dim_rows.get("finance_product_dim_v", 500))

    # ---- FK -> finance_customer_dim_v (finance_customer_id bigint) ----------
    gen = fk_bigint(gen, "customer_id", dim_rows.get("finance_customer_dim_v", 100))

    # ---- FK -> company_code (company_id bigint) ------------------------------
    gen = fk_bigint(gen, "company_id", dim_rows.get("company_code", 50))

    # ---- FK -> copa_attribution_dim (bigint PK) -----------------------------
    gen = fk_bigint(gen, "copa_attribution_id", dim_rows.get("copa_attribution_dim", 500))

    # ---- FK -> cost_center_dim_v (CC_000001 string PK) ----------------------
    # keep_id_col=True: DDL now exposes __cost_center_nbr_fk_id as a real column.
    gen = fk_string(gen, "cost_center_nbr", "CC",
                    dim_rows.get("cost_center_dim_v", 500), 6,
                    keep_id_col=True)

    # ---- FK -> geo_wholesale_value_business_dim (bigint PK) -----------------
    gen = fk_bigint(gen, "geo_wholesale_value_business_id",
                    dim_rows.get("geo_wholesale_value_business_dim", 200))

    # ---- FK -> geo_marketplace_channel_dim (bigint PK) ----------------------
    gen = fk_bigint(gen, "geo_marketplace_channel_id",
                    dim_rows.get("geo_marketplace_channel_dim", 100))

    # ---- FK -> gl_account_dim (GL_000001 string PK) -------------------------
    # keep_id_col=True: DDL now exposes __gl_account_nbr_fk_id as a real column.
    gen = fk_string(gen, "gl_account_nbr", "GL",
                    dim_rows.get("gl_account_dim", 200), 6,
                    keep_id_col=True)

    # ---- Non-FK reference: zfsm + fx-rate ids --------------------------------
    gen = gen.withColumn("zfsm_measure_id", T.LongType(),
                         minValue=1,
                         maxValue=dim_rows.get("gl_account_zfsm_measures_hierarchy_dim", 100),
                         random=True)

    # DDL order: etm/gaap fx-rate ids BEFORE the currency amounts
    gen = gen.withColumn("etm_foreign_currency_exchange_rate_id", T.LongType(),
                         minValue=1,
                         maxValue=dim_rows.get("finance_foreign_currency_exchange_rate", 200),
                         random=True)
    gen = gen.withColumn("gaap_foreign_currency_exchange_rate_id", T.LongType(),
                         minValue=1,
                         maxValue=dim_rows.get("finance_foreign_currency_exchange_rate", 200),
                         random=True)

    # ---- Measure columns (amounts) -------------------------------------------
    gen = decimal_amount(gen, "company_currency_amt",
                         min_val=-2_000_000.0, max_val=5_000_000.0, precision=28, scale=5)
    gen = decimal_amount(gen, "transaction_currency_amt",
                         min_val=-2_000_000.0, max_val=5_000_000.0, precision=28, scale=5)
    gen = decimal_amount(gen, "performance_management_currency_amt",
                         min_val=-2_000_000.0, max_val=5_000_000.0, precision=28, scale=5)

    # DDL order: etm_ind AFTER the currency amounts
    gen = gen.withColumn("etm_ind", T.IntegerType(), values=[0, 1], weights=[85, 15], random=True)

    gen = decimal_amount(gen, "sales_qty",
                         min_val=0.0, max_val=100_000.0, precision=28, scale=5)
    gen = decimal_amount(gen, "returns_qty",
                         min_val=0.0, max_val=10_000.0, precision=28, scale=5)

    # ---- Indicator flags -----------------------------------------------------
    gen = enum_col(gen, "general_ledger_fact_ind", ["Y", "N"], weights=[90, 10])
    gen = enum_col(gen, "cis_delta_ind", ["Y", "N", ""], weights=[10, 5, 85])
    gen = enum_col(gen, "general_ledger_ocogs_allocation_fact_ind",
                   ["Y", "N", ""], weights=[15, 5, 80])
    gen = enum_col(gen, "anaplan_corporate_ind", ["Y", "N", ""], weights=[20, 10, 70])

    # ---- Currency codes -------------------------------------------------------
    gen = enum_col(gen, "company_currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY", "CHF", "CAD", "AUD"],
                   weights=[35, 20, 10, 8, 10, 5, 7, 5])
    gen = enum_col(gen, "transaction_currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY", "CHF", "CAD", "AUD", "KRW", "BRL"],
                   weights=[30, 18, 8, 7, 8, 4, 6, 5, 7, 7])

    # ---- Denormalised dimension attributes (new) -----------------------------
    # These replicate natural keys from their source dims so aggregated tables
    # can GROUP BY or JOIN on them without looking up the dim each time.

    # Functional_Area_cd: matches functional_area.functional_area_cd (FA_0001..FA_NNNN)
    gen = fk_string(gen, "Functional_Area_cd", "FA",
                    dim_rows.get("functional_area", 100), 4)

    # accounting_document_type_cd: same value domain as the dim
    gen = enum_col(gen, "accounting_document_type_cd",
                   ["SA", "DA", "KA", "WA", "RV", "DR", "DG", "KG", "AB", "SB",
                    "RE", "KE", "ZA", "ZR", "ZD", "RA", "RF", "RD", "RS", "RT"])

    # profit_center_nbr: matches profit_center.profit_center_nbr (PC_000001..PC_NNNNNN)
    gen = fk_string(gen, "profit_center_nbr", "PC",
                    dim_rows.get("profit_center", 500), 6)

    # company_cd: derived from company_id so every fact row for the same
    # company gets the same 4-digit code, matching the dim's dddd template format.
    gen = gen.withColumn(
        "company_cd", T.StringType(),
        baseColumn="company_id",
        expr="lpad(string(company_id), 4, '0')",
    )

    return gen.build()


# ---------------------------------------------------------------------------
# CIS_fact
# ---------------------------------------------------------------------------

def gen_CIS_fact(
    spark: SparkSession,
    rows: int,
    partitions: int,
    dim_rows: dict,
) -> DataFrame:
    """
    Consolidated Income Statement fact.

    PK / integrity contract
    -----------------------
    * ``gl_account_id`` is the **PK** per ``schema.yaml`` -> generated as a
      unique sequential bigint ``[1..rows]``.
    * ``consolidated_income_statement_fact_id`` is a regular bigint reference,
      not the PK.
    * ``profit_center_id``           -> FK to ``profit_center.profit_center_id``
    * ``profit_center_nbr``          -> matches ``profit_center.profit_center_nbr``
    * ``functional_area_cd``         -> matches ``functional_area.functional_area_cd``
    * ``division_nbr``               -> matches ``division_text.division_nbr``
    * ``fiscal_year_period_nbr``     -> matches ``calendar_fiscal_period_v``
    * ``fiscal_yr`` is **derived** from ``fiscal_year_period_nbr`` so they
      remain consistent for downstream group-by aggregations.
    """
    gen = make_generator(spark, "CIS_fact", rows, partitions)

    # ---- PK (per schema) -----------------------------------------------------
    gen = pk_bigint(gen, "gl_account_id")

    # ---- FKs to dim natural keys --------------------------------------------
    gen = fk_string(gen, "profit_center_nbr", "PC",
                    dim_rows.get("profit_center", 500), 6)
    gen = fiscal_year_period(
        gen, "fiscal_year_period_nbr",
        count=dim_rows.get("calendar_fiscal_period_v", 108),
    )
    gen = _derive_fiscal_yr(gen)

    gen = decimal_amount(gen, "transaction_currency_amt", precision=18, scale=5)
    gen = fk_string(gen, "functional_area_cd", "FA",
                    dim_rows.get("functional_area", 100), 4)
    gen = fk_bigint(gen, "profit_center_id", dim_rows.get("profit_center", 500))
    gen = enum_col(gen, "division_nbr", _division_nbr_values(dim_rows))
    gen = gen.withColumn("segment_nbr", T.IntegerType(),
                         values=[1000, 2000, 3000, 4000, 5000, 6000], random=True)
    gen = gen.withColumn("partner_segment_nbr", T.IntegerType(),
                         values=[1000, 2000, 3000, 4000, 5000, 6000],
                         random=True, percentNulls=0.3)
    gen = template_string(gen, "document_type_cd", r"rr")
    gen = gen.withColumn("original_company_cd", T.IntegerType(),
                         minValue=1000, maxValue=9999, random=True)
    gen = decimal_amount(gen, "sign_adjusted_group_currency_amt", precision=18, scale=5)
    gen = decimal_amount(gen, "sign_adjusted_local_currency_amt", precision=18, scale=5)
    gen = decimal_amount(gen, "sign_adjusted_transaction_currency_amt", precision=18, scale=5)
    gen = decimal_amount(gen, "group_currency_amt", precision=18, scale=5)
    gen = decimal_amount(gen, "local_currency_amt", precision=18, scale=5)
    gen = decimal_amount(gen, "qty", min_val=0.0, max_val=50_000.0, precision=18, scale=5)
    # Non-PK reference id (random bigint; not unique).
    gen = gen.withColumn("consolidated_income_statement_fact_id", T.LongType(),
                         minValue=1, maxValue=max(1, rows), random=True)
    gen = template_string(gen, "financial_statement_item_cd", r"rrrrddd")
    gen = enum_col(gen, "local_currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY", "CHF", "CAD"],
                   weights=[35, 20, 10, 8, 10, 7, 10])
    gen = enum_col(gen, "transaction_currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY"],
                   weights=[35, 25, 15, 13, 12])
    gen = enum_col(gen, "version_nbr", ["001", "010", "BUD", "ACT", "FRC"], weights=[20, 20, 20, 20, 20])
    gen = fk_string(gen, "partner_profit_center_nbr", "PC",
                    dim_rows.get("profit_center", 500), 6)
    gen = template_string(gen, "partner_unit_cd", r"rrd", nullable_pct=0.4)
    gen = gen.withColumn("consolidation_unit_cd", T.IntegerType(),
                         minValue=1000, maxValue=9999, random=True)
    gen = enum_col(gen, "ledger_cd", ["0L", "1L", "2L", "A1", "B1"], weights=[35, 25, 20, 10, 10])
    gen = template_string(gen, "dimension_cd", r"rrd")
    gen = enum_col(gen, "record_type_cd", ["0", "1", "2", "3"], weights=[50, 25, 15, 10])
    gen = template_string(gen, "consolidation_group_cd", r"rrrrd")
    gen = gen.withColumn("consolidation_of_investment_activity_nbr", T.IntegerType(),
                         minValue=0, maxValue=9, random=True)
    gen = enum_col(gen, "chart_of_accounts_cd", ["GLBL", "US01", "EU01", "AP01"], weights=[40, 20, 25, 15])
    gen = gen.withColumn("trading_partner_nbr", T.IntegerType(), minValue=1000, maxValue=9999, random=True,
                         percentNulls=0.5)
    gen = template_string(gen, "region_summary_product_group_cd", r"rrd")
    gen = enum_col(gen, "version_group_nm",
                   ["Actual", "Budget", "Forecast", "Rolling Forecast"],
                   weights=[40, 20, 25, 15])
    gen = template_string(gen, "consolidated_segment_nm", r"rrrrrrr rrrrrr")
    gen = decimal_amount(gen, "sign_adjusted_qty", min_val=-50_000.0, max_val=50_000.0, precision=18, scale=5)
    gen = template_string(gen, "user_nm", r"rrrrrrrr.rrrrrrr")
    gen = template_string(gen, "additional_operation_information_nm", r"rrrrrrrrrr rrrrrr", nullable_pct=0.5)
    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    gen = template_string(gen, "cis_store_cd", r"dddddd", nullable_pct=0.5)
    gen = enum_col(gen, "posting_level_cd", ["00", "10", "20", "30"], weights=[50, 25, 15, 10])
    gen = template_string(gen, "base_unit_of_measure_cd", r"rrr")
    gen = enum_col(gen, "foreign_exchange_type_cd", ["M", "B", "G", "E", "GAAP"], weights=[30, 20, 20, 15, 15])
    gen = enum_col(gen, "consolidated_channel_nm",
                   ["Wholesale", "Direct", "Digital", "Franchise"], weights=[30, 30, 25, 15])
    return gen.build()


# ---------------------------------------------------------------------------
# consolidated_balance_sheet_fact
# ---------------------------------------------------------------------------

def gen_consolidated_balance_sheet_fact(
    spark: SparkSession,
    rows: int,
    partitions: int,
    dim_rows: dict,
) -> DataFrame:
    """
    Consolidated Balance Sheet fact.

    Integrity contract
    ------------------
    * ``consolidated_balance_sheet_fact_id`` is the unique PK.
    * ``profit_center_nbr``        -> matches ``profit_center.profit_center_nbr``
    * ``functional_area_cd``       -> matches ``functional_area.functional_area_cd``
    * ``division_nbr``             -> matches ``division_text.division_nbr``
    * ``fiscal_year_period_nbr``   -> matches ``calendar_fiscal_period_v``
    * ``fiscal_yr`` is **derived** from ``fiscal_year_period_nbr``.
    """
    gen = make_generator(spark, "consolidated_balance_sheet_fact", rows, partitions)
    gen = pk_bigint(gen, "consolidated_balance_sheet_fact_id")
    gen = template_string(gen, "financial_statement_item_cd", r"rrrrddd")
    gen = fk_string(gen, "profit_center_nbr", "PC",
                    dim_rows.get("profit_center", 500), 6)
    gen = fk_string(gen, "functional_area_cd", "FA",
                    dim_rows.get("functional_area", 100), 4)
    gen = enum_col(gen, "local_currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY"], weights=[35, 25, 15, 13, 12])
    gen = enum_col(gen, "transaction_currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY"], weights=[35, 25, 15, 13, 12])
    gen = enum_col(gen, "version_nbr", ["001", "010", "BUD", "ACT", "FRC"], weights=[20, 20, 20, 20, 20])
    gen = enum_col(gen, "division_nbr", _division_nbr_values(dim_rows))
    gen = fiscal_year_period(
        gen, "fiscal_year_period_nbr",
        count=dim_rows.get("calendar_fiscal_period_v", 108),
    )
    gen = template_string(gen, "partner_unit_cd", r"rrd", nullable_pct=0.4)
    gen = enum_col(gen, "posting_level_cd", ["00", "10", "20", "30"], weights=[50, 25, 15, 10])
    gen = template_string(gen, "document_type_cd", r"rr")
    gen = gen.withColumn("consolidation_unit_cd", T.IntegerType(), minValue=1000, maxValue=9999, random=True)
    gen = fk_string(gen, "partner_profit_center_nbr", "PC",
                    dim_rows.get("profit_center", 500), 6)
    gen = gen.withColumn("trading_partner_nbr", T.IntegerType(), minValue=1000, maxValue=9999,
                         random=True, percentNulls=0.5)
    gen = template_string(gen, "region_summary_product_group_cd", r"rrd")
    gen = decimal_amount(gen, "transaction_currency_amt", precision=18, scale=5)
    gen = decimal_amount(gen, "local_currency_amt", precision=18, scale=5)
    gen = decimal_amount(gen, "group_currency_amt", precision=18, scale=5)
    gen = gen.withColumn("partner_segment_nbr", T.IntegerType(),
                         values=[1000, 2000, 3000, 4000, 5000], random=True, percentNulls=0.3)
    gen = template_string(gen, "segment_nbr", r"dddd")
    gen = decimal_amount(gen, "qty", min_val=0.0, max_val=50_000.0, precision=18, scale=5)
    gen = template_string(gen, "consolidated_segment_nm", r"rrrrrrr rrrrrr")
    gen = enum_col(gen, "consolidated_channel_nm",
                   ["Wholesale", "Direct", "Digital", "Franchise"], weights=[30, 30, 25, 15])
    gen = _derive_fiscal_yr(gen)
    gen = enum_col(gen, "version_group_nm",
                   ["Actual", "Budget", "Forecast", "Rolling Forecast"], weights=[40, 20, 25, 15])
    gen = template_string(gen, "user_nm", r"rrrrrrrr.rrrrrrr")
    gen = template_string(gen, "additional_operation_information_nm", r"rrrrrrrrrr rrrrrr", nullable_pct=0.5)
    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    gen = date_col(gen, "_acdocu_latest_load_timestamp", begin="2023-01-01", end="2025-12-31")
    gen = enum_col(gen, "group_currency_cd", ["USD", "EUR"], weights=[80, 20])
    gen = decimal_amount(gen, "ending_balance_amt", precision=18, scale=5)
    gen = enum_col(gen, "foreign_exchange_type_cd", ["M", "B", "G", "E", "GAAP"], weights=[30, 20, 20, 15, 15])
    return gen.build()
