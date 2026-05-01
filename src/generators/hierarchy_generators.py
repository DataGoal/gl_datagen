"""
src/generators/hierarchy_generators.py
---------------------------------------
Generators for all hierarchy and reference tables.
These are generally independent tables (not FK-referenced by the fact) so they
can be generated in any order without referential integrity constraints.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from utils.datagen_helpers import (
    active_ind_col,
    date_col,
    decimal_amount,
    enum_col,
    make_generator,
    pk_bigint,
    pk_string,
    physical_source_col,
    template_string,
    user_id_col,
)


def _gen_cost_center_hierarchy(
    spark: SparkSession,
    table_name: str,
    rows: int,
    partitions: int,
    pk_col: str = "cost_center_hierarchy_hist_id",
) -> DataFrame:
    """Shared builder for cost_center hierarchy tables (30 levels)."""
    gen = make_generator(spark, table_name, rows, partitions)
    gen = pk_bigint(gen, pk_col)
    gen = template_string(gen, "cost_center_nbr", r"CC_dddddd")
    gen = enum_col(gen, "cost_center_hierarchy_nm",
                   ["Corporate", "EMEA", "NA", "APLA", "GC", "DTC", "Wholesale"],
                   weights=[15, 20, 20, 15, 15, 10, 5])

    for lvl in range(1, 31):
        gen = template_string(gen, f"cost_center_level_{lvl}_cd", r"rrddd", nullable_pct=max(0, (lvl - 5) * 0.08))
        gen = template_string(gen, f"cost_center_level_{lvl}_nm", r"rrrrrrr rrrrrr", nullable_pct=max(0, (lvl - 5) * 0.08))

    gen = enum_col(gen, "controlling_area_cd",
                   ["NA01", "EU01", "AP01", "CA01", "LA01", "GC01"], weights=[30, 25, 15, 10, 10, 10])
    return gen.build()


def _gen_profit_center_hierarchy(
    spark: SparkSession,
    table_name: str,
    rows: int,
    partitions: int,
    pk_col: str = "profit_center_hierarchy_id",
    pc_nbr_col: str = "profit_center_nbr",
    extra_cols: list = None,
) -> DataFrame:
    """Shared builder for profit_center hierarchy tables (9 levels)."""
    gen = make_generator(spark, table_name, rows, partitions)
    gen = pk_bigint(gen, pk_col)

    if extra_cols:
        for col_name, col_type, col_kwargs in extra_cols:
            gen = gen.withColumn(col_name, col_type, **col_kwargs)

    gen = template_string(gen, pc_nbr_col, r"PC_dddddd")
    gen = enum_col(gen, "controlling_area_cd",
                   ["NA01", "EU01", "AP01", "CA01", "LA01", "GC01"], weights=[30, 25, 15, 10, 10, 10])
    gen = enum_col(gen, "profit_center_hierarchy_nm",
                   ["Corporate", "EMEA PC", "NA PC", "APLA PC", "GC PC"], weights=[20, 25, 25, 15, 15])

    for lvl in range(1, 10):
        gen = template_string(gen, f"profit_center_level_{lvl}_cd", r"rrddd", nullable_pct=max(0, (lvl - 3) * 0.15))
        gen = template_string(gen, f"profit_center_level_{lvl}_nm", r"rrrrrrr rrrrrr", nullable_pct=max(0, (lvl - 3) * 0.15))

    return gen.build()


# ---------------------------------------------------------------------------
# atscale_geo_security
# ---------------------------------------------------------------------------

def gen_atscale_geo_security(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    regions = ["North America", "EMEA", "Greater China", "APLA", "Europe", "Latin America",
               "Asia Pacific", "Middle East", "Africa", "Global"]
    roles = ["Finance_NA", "Finance_EMEA", "Finance_GC", "Finance_APLA", "Finance_Global",
             "Finance_ReadOnly", "Finance_Admin", "Finance_Manager"]
    gen = make_generator(spark, "atscale_geo_security", rows, partitions)
    gen = gen.withColumn("region", T.StringType(), values=regions[:rows], uniqueValues=min(rows, len(regions)),
                         random=False)
    gen = gen.withColumn("role", T.StringType(), values=roles, random=True)
    return gen.build()


# ---------------------------------------------------------------------------
# consolidation_functional_area_hierarchy
# ---------------------------------------------------------------------------

def gen_consolidation_functional_area_hierarchy(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "consolidation_functional_area_hierarchy", rows, partitions)
    gen = pk_bigint(gen, "consolidation_functional_area_hierarchy_id")
    gen = active_ind_col(gen)
    gen = template_string(gen, "functional_area_cd", r"FA_dddd")
    gen = template_string(gen, "consolidation_functional_area_hierarchy_parent_cd", r"rrddd")
    gen = gen.withColumn("functional_area_level_nbr", T.IntegerType(), minValue=1, maxValue=7, random=True)
    gen = template_string(gen, "functional_area_type_nm", r"rrrrrrrrr rrrr")
    gen = template_string(gen, "consolidation_functional_area_hierarchy_cd", r"rrddd")
    gen = template_string(gen, "consolidation_functional_area_hierarchy_desc", r"rrrrrrrrr rrrrrrrr")

    for lvl in range(1, 8):
        gen = template_string(gen, f"consolidation_functional_area_{lvl}_cd", r"rrddd", nullable_pct=max(0, (lvl - 3) * 0.2))
        gen = template_string(gen, f"consolidation_functional_area_{lvl}_nm", r"rrrrrrr rrrrrr", nullable_pct=max(0, (lvl - 3) * 0.2))

    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    gen = date_col(gen, "record_created_tmst_utc")
    gen = date_col(gen, "record_update_tmst_utc")
    gen = date_col(gen, "_consolidation_functional_area_hierarchy_raw_latest_load_tmst",
                   begin="2023-01-01", end="2025-12-31")
    return gen.build()


# ---------------------------------------------------------------------------
# consolidation_segment_hierarchy_dim
# ---------------------------------------------------------------------------

def gen_consolidation_segment_hierarchy_dim(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "consolidation_segment_hierarchy_dim", rows, partitions)
    gen = pk_bigint(gen, "consolidation_segment_hierarchy_id")
    gen = gen.withColumn("segment_nbr", T.IntegerType(),
                         values=[1000, 2000, 3000, 4000, 5000, 6000, 7000], random=True)
    gen = template_string(gen, "segment_desc", r"rrrrrrr rrrrrrrr")
    gen = template_string(gen, "consolidation_segment_hierarchy_parent_cd", r"rrddd")
    gen = enum_col(gen, "segment_type_nm", ["Product", "Geography", "Channel", "Function"], weights=[30, 30, 25, 15])
    gen = gen.withColumn("segment_level_nbr", T.IntegerType(), minValue=1, maxValue=10, random=True)
    gen = template_string(gen, "consolidation_segment_hierarchy_cd", r"rrddd")
    gen = template_string(gen, "consolidation_segment_hierarchy_desc", r"rrrrrrrrr rrrrrrrr")
    gen = template_string(gen, "consolidation_segment_hierarchy_cd_desc", r"rrddd rrrrrrrrr")

    for lvl in range(1, 11):
        gen = template_string(gen, f"consolidation_segment_{lvl}_cd", r"rrddd", nullable_pct=max(0, (lvl - 4) * 0.15))
        gen = template_string(gen, f"consolidation_segment_{lvl}_nm", r"rrrrrrr rrrrrr", nullable_pct=max(0, (lvl - 4) * 0.15))
        gen = template_string(gen, f"consolidation_segment_{lvl}_cd_nm", r"rrddd rrrrrrr", nullable_pct=max(0, (lvl - 4) * 0.15))

    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    gen = active_ind_col(gen)
    return gen.build()


# ---------------------------------------------------------------------------
# segment_cost_center_hierarchy_dim_v
# ---------------------------------------------------------------------------

def gen_segment_cost_center_hierarchy_dim_v(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    return _gen_cost_center_hierarchy(spark, "segment_cost_center_hierarchy_dim_v", rows, partitions,
                                     pk_col="cost_center_hierarchy_hist_id")


# ---------------------------------------------------------------------------
# segment_profit_center_hierarchy
# ---------------------------------------------------------------------------

def gen_segment_profit_center_hierarchy(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "segment_profit_center_hierarchy", rows, partitions)
    gen = pk_string(gen, "segment_profit_center_nbr", "SPC", 6)
    gen = enum_col(gen, "profit_center_hierarchy_nm",
                   ["Segment NA", "Segment EMEA", "Segment GC", "Segment APLA"], weights=[30, 25, 25, 20])
    for lvl in range(1, 10):
        gen = template_string(gen, f"profit_center_level_{lvl}_cd", r"rrddd", nullable_pct=max(0, (lvl - 3) * 0.15))
        gen = template_string(gen, f"profit_center_level_{lvl}_nm", r"rrrrrrr rrrrrr", nullable_pct=max(0, (lvl - 3) * 0.15))
    return gen.build()


# ---------------------------------------------------------------------------
# DisChannel_cost_center_hierarchy_dim_v
# ---------------------------------------------------------------------------

def gen_DisChannel_cost_center_hierarchy_dim_v(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    return _gen_cost_center_hierarchy(spark, "DisChannel_cost_center_hierarchy_dim_v", rows, partitions)


# ---------------------------------------------------------------------------
# DisChannel_profit_center_hierarchy
# ---------------------------------------------------------------------------

def gen_DisChannel_profit_center_hierarchy(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "DisChannel_profit_center_hierarchy", rows, partitions)
    gen = pk_bigint(gen, "profit_center_hierarchy_id")
    gen = template_string(gen, "distrchnl_profit_center_nbr", r"PC_dddddd")
    gen = enum_col(gen, "controlling_area_cd",
                   ["NA01", "EU01", "AP01", "CA01", "LA01", "GC01"], weights=[30, 25, 15, 10, 10, 10])
    gen = enum_col(gen, "profit_center_hierarchy_nm",
                   ["DisChannel NA", "DisChannel EMEA", "DisChannel GC", "DisChannel APLA"], weights=[30, 25, 25, 20])
    for lvl in range(1, 10):
        gen = template_string(gen, f"profit_center_level_{lvl}_cd", r"rrddd", nullable_pct=max(0, (lvl - 3) * 0.15))
        gen = template_string(gen, f"profit_center_level_{lvl}_nm", r"rrrrrrr rrrrrr", nullable_pct=max(0, (lvl - 3) * 0.15))
    return gen.build()


# ---------------------------------------------------------------------------
# PartDisChannel_profit_center_hierarchy
# ---------------------------------------------------------------------------

def gen_PartDisChannel_profit_center_hierarchy(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "PartDisChannel_profit_center_hierarchy", rows, partitions)
    gen = pk_bigint(gen, "profit_center_hierarchy_id")
    gen = enum_col(gen, "controlling_area_cd",
                   ["NA01", "EU01", "AP01", "CA01", "LA01", "GC01"], weights=[30, 25, 15, 10, 10, 10])
    gen = enum_col(gen, "profit_center_hierarchy_nm",
                   ["PartDisCh NA", "PartDisCh EMEA", "PartDisCh GC", "PartDisCh APLA"], weights=[30, 25, 25, 20])
    for lvl in range(1, 10):
        gen = template_string(gen, f"profit_center_level_{lvl}_cd", r"rrddd", nullable_pct=max(0, (lvl - 3) * 0.15))
        gen = template_string(gen, f"profit_center_level_{lvl}_nm", r"rrrrrrr rrrrrr", nullable_pct=max(0, (lvl - 3) * 0.15))
    gen = template_string(gen, "prtrdistrchnl_profit_center_nbr", r"PC_dddddd")
    return gen.build()


# ---------------------------------------------------------------------------
# division_text_dim_v
# ---------------------------------------------------------------------------

def gen_division_text_dim_v(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    from src.generators.dimension_generators import gen_division_text
    raw = gen_division_text(spark, rows, partitions)
    return raw.select("division_nbr", "division_nm", "division_id", "division_group")


# ---------------------------------------------------------------------------
# gl_account_hierarchy
# ---------------------------------------------------------------------------

def _gen_gl_account_hierarchy(
    spark: SparkSession,
    table_name: str,
    rows: int,
    partitions: int,
    pk_col: str = "gl_account_hierarchy_id",
    extra_string_cols: list = None,
) -> DataFrame:
    gen = make_generator(spark, table_name, rows, partitions)
    gen = pk_bigint(gen, pk_col)
    gen = template_string(gen, "gl_account_nbr", r"GL_dddddd")
    gen = enum_col(gen, "hierarchy_chart_of_accounts_cd",
                   ["GLBL", "US01", "EU01", "AP01"], weights=[40, 20, 25, 15])
    gen = enum_col(gen, "hierarchy_nm",
                   ["GAAP Hierarchy", "IFRS Hierarchy", "Management Hierarchy",
                    "Consolidation Hierarchy", "Statutory Hierarchy"],
                   weights=[25, 20, 25, 20, 10])

    for ind in ["hierarchy_liabilty_cd", "hierarchy_net_loss_cd", "hierarchy_net_profit_cd",
                "hierarchy_profit_loss_cd", "hierarchy_not_assignable_asset_cd", "hierarchy_notes_cd"]:
        gen = template_string(gen, ind, r"rrddd", nullable_pct=0.5)

    gen = template_string(gen, "hierarchy_cd", r"rrddd")
    gen = template_string(gen, "parent_cd", r"rrddd", nullable_pct=0.1)
    gen = template_string(gen, "node_nm", r"rrrrrrr rrrrrr")
    gen = template_string(gen, "node_cd", r"rrddd")
    gen = gen.withColumn("node_gl_account_nbr", T.IntegerType(), minValue=100000, maxValue=999999, random=True)
    gen = gen.withColumn("node_gl_account_to_nbr", T.IntegerType(), minValue=100000, maxValue=999999,
                         random=True, percentNulls=0.3)
    gen = enum_col(gen, "functional_area_assignment_allowed_ind", ["Y", "N"], weights=[70, 30])
    gen = enum_col(gen, "consolidation_chart_of_accounts_used_ind", ["Y", "N"], weights=[60, 40])
    gen = gen.withColumn("depth_of_leaf_nbr", T.IntegerType(), minValue=1, maxValue=30, random=True)
    gen = gen.withColumn("depth_of_tree_nbr", T.IntegerType(), minValue=1, maxValue=30, random=True)

    for lvl in range(1, 31):
        gen = template_string(gen, f"gl_account_level_{lvl}_cd", r"dddddd", nullable_pct=max(0, (lvl - 8) * 0.06))
        gen = template_string(gen, f"gl_account_level_{lvl}_nm", r"rrrrrrrrrr rrrrrrrr", nullable_pct=max(0, (lvl - 8) * 0.06))

    gen = enum_col(gen, "hierarchy_language_cd", ["EN", "DE", "FR"], weights=[60, 25, 15])
    gen = template_string(gen, "hierarchy_action_cd", r"rrd")
    gen = template_string(gen, "hierarchy_category_cd", r"rrd")
    gen = template_string(gen, "top_level_assets_financial_reporting_structure_item_cd", r"rrddd")
    gen = template_string(gen, "recipient_business_system_id", r"rrrr_ddd")
    gen = template_string(gen, "sender_business_system_id", r"rrrr_ddd")

    for ind in ["item_list_complete_transmission_ind", "hierarchy_name_list_complete_transmission_ind",
                "node_name_list_complete_transmission_ind", "sign_reversed_ind",
                "functional_area_list_complete_transmission_ind",
                "gl_account_list_complete_transmission_ind",
                "relationship_list_complete_transmission_ind", "totals_visible_ind",
                "credit_balance_ind", "debit_balance_ind"]:
        gen = enum_col(gen, ind, ["Y", "N"], weights=[70, 30])

    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)

    if extra_string_cols:
        for col_name in extra_string_cols:
            gen = template_string(gen, col_name, r"rrddd rrrrrrr")

    if table_name == "gl_account_hierarchy":
        gen = gen.withColumn("gl_account_hierarchy_hist_id", T.LongType(),
                             minValue=1, maxValue=rows, random=True)
        gen = date_col(gen, "record_created_tmst_utc")
        gen = date_col(gen, "record_update_tmst_utc")
        gen = date_col(gen, "_gl_account_hierarchy_cleansed_latest_load_timestamp",
                       begin="2023-01-01", end="2025-12-31")

    return gen.build()


def gen_gl_account_hierarchy(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    return _gen_gl_account_hierarchy(spark, "gl_account_hierarchy", rows, partitions)


def gen_management_gl_account_hierarchy(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    return _gen_gl_account_hierarchy(spark, "management_gl_account_hierarchy", rows, partitions,
                                     extra_string_cols=["hierarchy_cd_nm"])


# ---------------------------------------------------------------------------
# gl_account_zfsm_measures_hierarchy_dim
# ---------------------------------------------------------------------------

def gen_gl_account_zfsm_measures_hierarchy_dim(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "gl_account_zfsm_measures_hierarchy_dim", rows, partitions)
    gen = pk_bigint(gen, "zfsm_measure_id")
    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    gen = active_ind_col(gen)
    gen = template_string(gen, "zfsm_measure_cd", r"ZFSM_rrd")
    gen = template_string(gen, "zfsm_measure_desc", r"rrrrrrrrr rrrrrrrr rrrr")
    for lvl in range(1, 14):
        gen = template_string(gen, f"gl_account_level_{lvl}_cd", r"dddddd", nullable_pct=max(0, (lvl - 5) * 0.1))
        gen = template_string(gen, f"gl_account_level_{lvl}_nm", r"rrrrrrrrrr rrrrrrrr", nullable_pct=max(0, (lvl - 5) * 0.1))
    gen = date_col(gen, "record_created_tmst_utc")
    gen = date_col(gen, "record_update_tmst_utc")
    return gen.build()


# ---------------------------------------------------------------------------
# finance_foreign_currency_exchange_rate
# ---------------------------------------------------------------------------

def gen_finance_foreign_currency_exchange_rate(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    currencies = ["USD", "EUR", "GBP", "JPY", "CNY", "CHF", "CAD", "AUD", "KRW",
                  "BRL", "MXN", "INR", "SEK", "NOK", "DKK", "SGD", "HKD", "NZD"]
    currency_names = {
        "USD": "US Dollar", "EUR": "Euro", "GBP": "British Pound",
        "JPY": "Japanese Yen", "CNY": "Chinese Yuan", "CHF": "Swiss Franc",
        "CAD": "Canadian Dollar", "AUD": "Australian Dollar", "KRW": "Korean Won",
        "BRL": "Brazilian Real", "MXN": "Mexican Peso", "INR": "Indian Rupee",
        "SEK": "Swedish Krona", "NOK": "Norwegian Krone", "DKK": "Danish Krone",
        "SGD": "Singapore Dollar", "HKD": "Hong Kong Dollar", "NZD": "New Zealand Dollar"
    }
    gen = make_generator(spark, "finance_foreign_currency_exchange_rate", rows, partitions)
    gen = pk_bigint(gen, "finance_foreign_currency_exchange_rate_id")
    gen = enum_col(gen, "from_currency_cd", currencies)
    gen = enum_col(gen, "exchange_rate_cd", ["M", "B", "G", "E", "GAAP"], weights=[30, 20, 20, 15, 15])
    gen = enum_col(gen, "exchange_rate_nm",
                   ["Monthly Average", "Budget Rate", "GAAP Rate", "End of Period", "Reporting Rate"],
                   weights=[30, 20, 20, 15, 15])
    gen = decimal_amount(gen, "exchange_rate", min_val=0.0001, max_val=200.0,
                         precision=38, scale=18)
    gen = enum_col(gen, "from_currency_nm", list(currency_names.values()))
    gen = enum_col(gen, "to_currency_cd", ["USD", "EUR"], weights=[80, 20])
    gen = enum_col(gen, "to_currency_nm", ["US Dollar", "Euro"], weights=[80, 20])
    gen = active_ind_col(gen)
    return gen.build()


# ---------------------------------------------------------------------------
# retail_global_store_profile_v
# ---------------------------------------------------------------------------

def gen_retail_global_store_profile_v(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "retail_global_store_profile_v", rows, partitions)
    gen = template_string(gen, "address_match_type_name", r"rrrrrrrrr")
    gen = template_string(gen, "store_uuid", r"dddddddd-dddd-dddd-dddd-dddddddddddd")
    gen = template_string(gen, "application_reason_desc", r"rrrrrrrrr rrrrrr", nullable_pct=0.4)
    gen = enum_col(gen, "brand_cd", ["NK", "JD", "CV", "HU"], weights=[65, 20, 10, 5])
    gen = enum_col(gen, "brand_desc", ["Nike", "Jordan", "Converse", "Hurley"], weights=[65, 20, 10, 5])
    gen = gen.withColumn("building_floor_nbr", T.IntegerType(), minValue=1, maxValue=5, random=True,
                         percentNulls=0.3)
    gen = date_col(gen, "change_timestamp")
    gen = template_string(gen, "china_store_sub_channel_cd", r"rrd", nullable_pct=0.7)
    gen = template_string(gen, "china_store_sub_channel_desc", r"rrrrrrrrr rrrr", nullable_pct=0.7)
    gen = template_string(gen, "city_name", r"rrrrrrrrr")
    gen = template_string(gen, "connect_global_store_key_cd", r"dddddddddd")
    gen = enum_col(gen, "country_cd",
                   ["US", "DE", "GB", "FR", "CN", "JP", "KR", "AU", "CA", "BR", "MX"],
                   weights=[30, 8, 7, 7, 10, 5, 3, 3, 5, 4, 4])
    gen = enum_col(gen, "country_name",
                   ["United States", "Germany", "United Kingdom", "France", "China",
                    "Japan", "Korea", "Australia", "Canada", "Brazil", "Mexico"],
                   weights=[30, 8, 7, 7, 10, 5, 3, 3, 5, 4, 4])
    gen = date_col(gen, "create_dt")
    gen = template_string(gen, "customer_city_local_name", r"rrrrrrrrr", nullable_pct=0.6)
    gen = template_string(gen, "customer_id", r"ddddddddddd")
    gen = template_string(gen, "customer_local_province_state_name", r"rrrrrrrrr", nullable_pct=0.6)
    gen = template_string(gen, "customer_nbr", r"dddddddd")
    gen = template_string(gen, "customer_ship_to_name", r"rrrrrrrr rrrrrrr")
    gen = template_string(gen, "dma_cd", r"ddddd", nullable_pct=0.5)
    gen = template_string(gen, "dma_desc", r"rrrrrrrrr rrrrrr", nullable_pct=0.5)
    gen = template_string(gen, "fixture_type_name", r"rrrrrrrrr", nullable_pct=0.4)
    gen = template_string(gen, "fixture_program_name", r"rrrrrrrrrr rrrr", nullable_pct=0.4)
    gen = enum_col(gen, "geography_region_name",
                   ["North America", "EMEA", "Greater China", "APLA", "Europe"], weights=[30, 25, 15, 15, 15])
    gen = template_string(gen, "global_store_channel_class_cd", r"rrd")
    gen = template_string(gen, "global_store_channel_class_desc", r"rrrrrrrrrr rrrr")
    gen = template_string(gen, "greater_china_sub_territory_name", r"rrrrrrrr", nullable_pct=0.8)
    gen = template_string(gen, "__id_value", r"ddddddddddddddddd")
    gen = template_string(gen, "global_key_city_name", r"rrrrrrrrr", nullable_pct=0.5)
    gen = template_string(gen, "landlord", r"rrrrrrrr rrrrrrrrrrr", nullable_pct=0.3)
    gen = date_col(gen, "change_dt")
    gen = date_col(gen, "lease_expiration_dt", begin="2023-01-01", end="2035-12-31")
    gen = template_string(gen, "lease_status_cd", r"rrd")
    gen = template_string(gen, "lease_status_desc", r"rrrrrrrrr")
    gen = template_string(gen, "local_language_store_street_address1", r"ddd rrrrrrr rrrrrr", nullable_pct=0.4)
    gen = template_string(gen, "local_language_store_street_address2", r"rrrrrrr rrd", nullable_pct=0.7)
    gen = template_string(gen, "location_name", r"rrrrrrrrrr rrrrrrr")
    gen = enum_col(gen, "mall_grade", ["A", "A+", "B", "B+", "C"], weights=[25, 20, 25, 20, 10])
    gen = template_string(gen, "ncx_concept", r"rrrrrrrr", nullable_pct=0.5)
    gen = gen.withColumn("nike_selling_space_size_quantity", T.FloatType(),
                         minValue=500.0, maxValue=50000.0, random=True)
    gen = gen.withColumn("original_pos_id", T.IntegerType(), minValue=100000, maxValue=999999, random=True)
    gen = template_string(gen, "planned_retail_concept", r"rrrrrrrr", nullable_pct=0.4)
    gen = gen.withColumn("pos_id", T.IntegerType(), minValue=100000, maxValue=999999, random=True)
    gen = gen.withColumn("pos_retailer_location_id", T.IntegerType(), minValue=100000, maxValue=999999,
                         random=True, percentNulls=0.2)
    gen = template_string(gen, "postal_cd", r"ddddd")
    gen = template_string(gen, "province_state_name", r"rrrrrrrrrr")
    gen = template_string(gen, "real_estate_category_cd", r"rrd")
    gen = template_string(gen, "real_estate_category_desc", r"rrrrrrrrr rrrrr")
    gen = enum_col(gen, "record_type", ["STORE", "OUTLET", "INLINE", "DIGITAL"], weights=[50, 20, 20, 10])
    gen = date_col(gen, "retail_concept_actual_dt", nullable_pct=0.3)
    gen = template_string(gen, "retail_concept_cd", r"rrd")
    gen = template_string(gen, "retail_concept_desc", r"rrrrrrrrr rrrr")
    gen = enum_col(gen, "retail_concept_size", ["XS", "S", "M", "L", "XL", "XXL"], weights=[5, 10, 25, 30, 20, 10])
    gen = date_col(gen, "retail_concept_target_dt", nullable_pct=0.3)
    gen = enum_col(gen, "retail_concept_volume", ["Low", "Medium", "High", "Premium"], weights=[15, 35, 35, 15])
    gen = template_string(gen, "ship_to_customer_nbr", r"dddddddd")
    gen = enum_col(gen, "space_uom", ["SQFT", "SQM"], weights=[60, 40])
    gen = gen.withColumn("store_city_tier_nbr", T.IntegerType(), minValue=1, maxValue=5, random=True)
    gen = date_col(gen, "store_close_dt", begin="2020-01-01", end="2025-12-31", nullable_pct=0.8)
    gen = template_string(gen, "store_closure_reason_desc", r"rrrrrrrrr rrrrrr", nullable_pct=0.8)
    gen = template_string(gen, "store_cd", r"dddddd")
    gen = template_string(gen, "store_distribution_type_cd", r"rrd")
    gen = template_string(gen, "store_distribution_type_desc", r"rrrrrrrrr rrrr")
    gen = template_string(gen, "store_district_name", r"rrrrrrrrr rrrr")
    gen = template_string(gen, "store_environment_cd", r"rrd")
    gen = template_string(gen, "store_environment_desc", r"rrrrrrrrr")
    gen = template_string(gen, "store_front_name_english", r"rrrrrrrrrr rrrrrr")
    gen = template_string(gen, "store_front_name_local", r"rrrrrrrrrr rrrrrr", nullable_pct=0.5)
    gen = template_string(gen, "store_hotspot_id", r"rrddd", nullable_pct=0.5)
    gen = template_string(gen, "store_hotspot_name_english", r"rrrrrrrrr rrrr", nullable_pct=0.5)
    gen = template_string(gen, "store_hotspot_name_local", r"rrrrrrrrr rrrr", nullable_pct=0.5)
    gen = template_string(gen, "store_hotspot_tier_desc", r"rrrrrrrrrr", nullable_pct=0.5)
    gen = gen.withColumn("store_id_kona", T.IntegerType(), minValue=10000, maxValue=99999, random=True)
    gen = date_col(gen, "store_last_open_dt", begin="2000-01-01", end="2025-12-31")
    gen = date_col(gen, "store_last_renovation_dt", begin="2010-01-01", end="2025-12-31", nullable_pct=0.5)
    gen = template_string(gen, "store_last_renovation_type_cd", r"rrd", nullable_pct=0.5)
    gen = template_string(gen, "store_last_renovation_type_desc", r"rrrrrrrrr", nullable_pct=0.5)
    gen = template_string(gen, "store_lead_category_cd", r"rrd")
    gen = template_string(gen, "store_lead_category_name", r"rrrrrrrrrr rrrrr")
    gen = gen.withColumn("store_level", T.IntegerType(), minValue=1, maxValue=5, random=True)
    gen = template_string(gen, "store_name_english", r"rrrrrrrrrr rrrrrr")
    gen = template_string(gen, "store_name_local", r"rrrrrrrrrr rrrrrr", nullable_pct=0.4)
    gen = date_col(gen, "store_open_dt", begin="2000-01-01", end="2023-12-31")
    gen = enum_col(gen, "store_priority", ["P1", "P2", "P3", "P4"], weights=[20, 30, 30, 20])
    gen = gen.withColumn("store_selling_space_size_quantity", T.FloatType(),
                         minValue=300.0, maxValue=30000.0, random=True)
    gen = enum_col(gen, "store_status_cd", ["OPEN", "CLSD", "TEMP", "PLAN"], weights=[75, 10, 5, 10])
    gen = enum_col(gen, "store_status_desc", ["Open", "Closed", "Temporarily Closed", "Planned"],
                   weights=[75, 10, 5, 10])
    gen = template_string(gen, "store_street_address1", r"ddd rrrrrrr rrrrrr")
    gen = template_string(gen, "store_street_address2", r"rrrrrrr rrd", nullable_pct=0.6)
    gen = template_string(gen, "store_sub_type_cd", r"rrd")
    gen = template_string(gen, "store_sub_type_desc", r"rrrrrrrrr")
    gen = enum_col(gen, "store_tier_desc", ["Tier 1", "Tier 2", "Tier 3", "Tier 4"], weights=[20, 30, 30, 20])
    gen = gen.withColumn("store_tier_id", T.IntegerType(), minValue=1, maxValue=4, random=True)
    gen = gen.withColumn("store_total_space_size", T.FloatType(),
                         minValue=500.0, maxValue=60000.0, random=True)
    gen = template_string(gen, "telephone_nbr", r"+d (ddd) ddd-dddd")
    gen = template_string(gen, "territory_name", r"rrrrrrrrr")
    gen = gen.withColumn("total_stories", T.IntegerType(), minValue=1, maxValue=5, random=True)
    gen = template_string(gen, "vendor_pos_key", r"rrddddddd")
    gen = enum_col(gen, "iso_country_cd",
                   ["US", "DE", "GB", "FR", "CN", "JP", "KR", "AU", "CA"],
                   weights=[30, 8, 7, 7, 10, 5, 3, 3, 7])
    gen = enum_col(gen, "default_transaction_iso_currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY", "CAD", "AUD"],
                   weights=[35, 20, 8, 8, 10, 10, 9])
    gen = template_string(gen, "store_short_name_english", r"rrrrrrrrrr")
    gen = template_string(gen, "store_master_id", r"rrd-dddddddddddd")
    gen = template_string(gen, "geography_abbrv", r"rrr")
    gen = date_col(gen, "stream_change_timestamp")
    gen = enum_col(gen, "space_uom_cd", ["SQFT", "SQM"], weights=[60, 40])
    gen = template_string(gen, "store_district_cd", r"rrddd")
    gen = template_string(gen, "region_cd", r"rrd")
    gen = enum_col(gen, "climate_cd", ["COLD", "TEMP", "WARM", "HOT", "TRPC"], weights=[20, 30, 25, 15, 10])
    gen = template_string(gen, "climate_name", r"rrrrrrrrrr")
    gen = enum_col(gen, "comparable_status", ["COMP", "NON-COMP", "NEW"], weights=[60, 25, 15])
    gen = template_string(gen, "real_estate_type_cd", r"rrd")
    gen = template_string(gen, "real_estate_type_name", r"rrrrrrrrr rrrr")
    gen = template_string(gen, "retail_sales_area_cd", r"rrddd")
    gen = template_string(gen, "retail_sales_area_name", r"rrrrrrrrr rrrrrr")
    gen = template_string(gen, "retail_sales_district_cd", r"rrddd")
    gen = template_string(gen, "retail_sales_district_name", r"rrrrrrrrr rrrrrr")
    gen = template_string(gen, "retail_sub_concept_cd", r"rrd")
    gen = template_string(gen, "retail_sub_concept_name", r"rrrrrrrrr")
    gen = template_string(gen, "sales_organization_cd", r"rrddd")
    gen = template_string(gen, "sales_organization_name", r"rrrrrrrrrr rrrrrrrrrr")
    gen = enum_col(gen, "record_type_cd", ["0", "1", "2", "3"], weights=[50, 25, 15, 10])
    gen = template_string(gen, "territory_cd", r"rrddd")
    gen = template_string(gen, "retail_sales_territory_cd", r"rrddd")
    gen = template_string(gen, "retail_sales_territory_name", r"rrrrrrrrr rrrrrr")
    gen = template_string(gen, "retail_store_district_cd", r"rrddd")
    gen = template_string(gen, "retail_store_district_name", r"rrrrrrrrr rrrrrr")
    gen = template_string(gen, "banner_division_text", r"rrrrrrrrrr")
    gen = template_string(gen, "marketplace_store_tier_cd", r"rrd")
    gen = template_string(gen, "marketplace_store_tier_name", r"rrrrrrrrr")
    gen = gen.withColumn("store_score", T.IntegerType(), minValue=1, maxValue=100, random=True)
    gen = template_string(gen, "partner_store_positioning_cd", r"rrd")
    gen = template_string(gen, "partner_store_positioning_name", r"rrrrrrrrr rrrr")
    gen = template_string(gen, "geo_key_city_cd", r"rrddd")
    gen = template_string(gen, "geo_key_city_name", r"rrrrrrrrr")
    gen = template_string(gen, "key_trade_zone_cd", r"rrddd")
    gen = template_string(gen, "key_trade_zone_name", r"rrrrrrrrr rrrrrr")
    df = gen.build()
    return df.withColumnRenamed("__id_value", "id")
