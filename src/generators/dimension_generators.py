"""
src/generators/dimension_generators.py
---------------------------------------
One function per FK-referenced dimension table.
Each function returns a Spark DataFrame ready to be written to Delta.

FK integrity contract
---------------------
Every dimension with a bigint PK generates PKs as a sequential range [1..rows].
The fact generator then samples random integers in [1..rows] to create valid FKs
without needing a broadcast join — which scales to 25B fact rows without memory issues.

Dimensions with string PKs (cost_center_dim_v, gl_account_dim) use a deterministic
template so the fact generator can reconstruct valid FK values from an integer seed.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from utils.datagen_helpers import (
    active_ind_col,
    date_col,
    decimal_amount,
    enum_col,
    fiscal_period_values,
    fk_bigint,
    make_generator,
    pk_bigint,
    pk_int,
    pk_string,
    physical_source_col,
    template_string,
    user_id_col,
)


# ---------------------------------------------------------------------------
# accounting_document_type
# ---------------------------------------------------------------------------

def gen_accounting_document_type(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    doc_types = ["SA", "DA", "KA", "WA", "RV", "DR", "DG", "KG", "AB", "SB",
                 "RE", "KE", "ZA", "ZR", "ZD", "RA", "RF", "RD", "RS", "RT"]
    doc_names = ["Customer Invoice", "Customer Credit Memo", "Vendor Invoice",
                 "Vendor Credit Memo", "Billing Document", "Customer Debit Memo",
                 "Customer Credit", "Vendor Credit", "Accounting Document",
                 "Accounting Sub-Document", "Invoice Receipt", "Accounting Credit",
                 "Internal Document A", "Internal Document R", "Internal Document D",
                 "Revenue Allocation", "Revenue Forecast", "Revenue Deferred",
                 "Revenue Supplement", "Revenue Transfer"]

    gen = make_generator(spark, "accounting_document_type", rows, partitions)
    gen = pk_bigint(gen, "accounting_document_type_id")
    gen = gen.withColumn("accounting_document_type_cd", T.StringType(),
                         values=doc_types[:rows if rows <= len(doc_types) else len(doc_types)],
                         uniqueValues=min(rows, len(doc_types)), random=True)
    gen = gen.withColumn("accounting_document_type_nm", T.StringType(),
                         values=doc_names, random=True)
    gen = active_ind_col(gen)
    gen = enum_col(gen, "language_cd", ["EN", "DE", "FR", "ES", "ZH", "JA"], weights=[50, 15, 10, 8, 10, 7])
    gen = gen.withColumn("accounting_document_type_cd_nm", T.StringType(),
                         baseColumn=["accounting_document_type_cd", "accounting_document_type_nm"],
                         expr="concat(accounting_document_type_cd, ' - ', accounting_document_type_nm)")
    return gen.build()


# ---------------------------------------------------------------------------
# calendar_fiscal_period_v
# ---------------------------------------------------------------------------

def gen_calendar_fiscal_period_v(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    """
    Generate realistic fiscal calendar periods.

    fiscal_year_period_nbr (PK) format: YYYYPP (e.g. 202403 = FY2024, Period 3).
    The set of values comes from :func:`fiscal_period_values` so the central fact
    table can sample from the exact same domain via ``fiscal_year_period(..., count=...)``.
    """
    import calendar
    import datetime

    period_values = fiscal_period_values(rows)

    records = []
    for period_id, fy_period in enumerate(period_values, start=1):
        yr = fy_period // 100
        mo = fy_period % 100
        cal_q = (mo - 1) // 3 + 1
        fiscal_q = cal_q  # simplification: fiscal Q = calendar Q
        start = datetime.date(yr, mo, 1)
        last_day = calendar.monthrange(yr, mo)[1]
        end = datetime.date(yr, mo, last_day)
        records.append((
            fy_period,                             # fiscal_year_period_nbr (PK)
            start.strftime("%B"),                  # month_long_nm
            start.strftime("%b"),                  # month_short_nm
            mo,                                    # month_nbr
            yr * 100 + mo,                         # year_mth
            start,                                 # month_relevance_dt
            start,                                 # month_start_dt
            end,                                   # month_end_dt
            period_id,                             # month_sort_sequence_nbr
            mo,                                    # fiscal_period_nbr
            f"P{mo:02d}",                          # fiscal_period_cd
            period_id,                             # fiscal_period_sort_sequence_nbr
            f"FY{yr}P{mo:02d}",                    # fiscal_year_period_cd
            f"FY{yr} Period {mo:02d}",             # fiscal_year_period_nm
            f"S{(mo-1)//6+1}",                     # season_period_cd
            f"S{(mo-1)//6+1}A",                    # season_alternate_period_cd
            "Fall/Winter" if mo >= 7 else "Spring/Summer",  # season_nm
            start,                                 # season_relevance_dt
            start,                                 # season_start_dt
            end,                                   # season_end_dt
            period_id,                             # season_sort_sequence_nbr
            cal_q,                                 # quarter_calendar_nbr
            cal_q,                                 # quarter_calendar_sequence_nbr
            cal_q,                                 # quarter_business_nbr
            fiscal_q,                              # fiscal_quarter_nbr
            f"Q{fiscal_q}",                        # fiscal_quarter_cd
            fiscal_q,                              # fiscal_quarter_sort_sequence_nbr
            yr * 10 + fiscal_q,                    # fiscal_year_quarter_nbr
            f"FY{yr}Q{fiscal_q}",                  # fiscal_year_quarter_cd
            f"FY{yr}-Q{fiscal_q}",                 # fiscal_year_quarter_alternate_cd
            str(yr),                               # year_cd
            f"FY{yr}",                             # year_nm
            str(yr),                               # year_nbr
            datetime.date(yr, 1, 1),               # year_start_dt
            datetime.date(yr, 12, 31),             # year_end_dt
            yr,                                    # business_year_nbr
            yr,                                    # fiscal_year_nbr
            f"FY{yr}",                             # fiscal_year_cd
            period_id,                             # fiscal_period_sort
        ))

    schema = T.StructType([
        T.StructField("fiscal_year_period_nbr", T.IntegerType(), False),
        T.StructField("month_long_nm", T.StringType(), True),
        T.StructField("month_short_nm", T.StringType(), True),
        T.StructField("month_nbr", T.IntegerType(), True),
        T.StructField("year_mth", T.IntegerType(), True),
        T.StructField("month_relevance_dt", T.DateType(), True),
        T.StructField("month_start_dt", T.DateType(), True),
        T.StructField("month_end_dt", T.DateType(), True),
        T.StructField("month_sort_sequence_nbr", T.IntegerType(), True),
        T.StructField("fiscal_period_nbr", T.IntegerType(), True),
        T.StructField("fiscal_period_cd", T.StringType(), True),
        T.StructField("fiscal_period_sort_sequence_nbr", T.IntegerType(), True),
        T.StructField("fiscal_year_period_cd", T.StringType(), True),
        T.StructField("fiscal_year_period_nm", T.StringType(), True),
        T.StructField("season_period_cd", T.StringType(), True),
        T.StructField("season_alternate_period_cd", T.StringType(), True),
        T.StructField("season_nm", T.StringType(), True),
        T.StructField("season_relevance_dt", T.DateType(), True),
        T.StructField("season_start_dt", T.DateType(), True),
        T.StructField("season_end_dt", T.DateType(), True),
        T.StructField("season_sort_sequence_nbr", T.IntegerType(), True),
        T.StructField("quarter_calendar_nbr", T.IntegerType(), True),
        T.StructField("quarter_calendar_sequence_nbr", T.IntegerType(), True),
        T.StructField("quarter_business_nbr", T.IntegerType(), True),
        T.StructField("fiscal_quarter_nbr", T.IntegerType(), True),
        T.StructField("fiscal_quarter_cd", T.StringType(), True),
        T.StructField("fiscal_quarter_sort_sequence_nbr", T.IntegerType(), True),
        T.StructField("fiscal_year_quarter_nbr", T.IntegerType(), True),
        T.StructField("fiscal_year_quarter_cd", T.StringType(), True),
        T.StructField("fiscal_year_quarter_alternate_cd", T.StringType(), True),
        T.StructField("year_cd", T.StringType(), True),
        T.StructField("year_nm", T.StringType(), True),
        T.StructField("year_nbr", T.StringType(), True),
        T.StructField("year_start_dt", T.DateType(), True),
        T.StructField("year_end_dt", T.DateType(), True),
        T.StructField("business_year_nbr", T.IntegerType(), True),
        T.StructField("fiscal_year_nbr", T.IntegerType(), True),
        T.StructField("fiscal_year_cd", T.StringType(), True),
        T.StructField("fiscal_period_sort", T.IntegerType(), True),
    ])
    return spark.createDataFrame(records, schema).repartition(max(1, partitions))


# ---------------------------------------------------------------------------
# profit_center
# ---------------------------------------------------------------------------

def gen_profit_center(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    geos = ["North America", "EMEA", "Greater China", "APLA", "Europe", "Latin America"]
    channels = ["Wholesale", "Direct", "Digital", "Marketplace", "Owned Retail"]
    territories = ["US West", "US East", "UK", "Germany", "France", "China", "Japan",
                   "Australia", "Canada", "Brazil", "Korea", "Netherlands"]
    segments = ["Running", "Training", "Basketball", "Football", "Sportswear", "Jordan"]

    gen = make_generator(spark, "profit_center", rows, partitions)
    gen = pk_bigint(gen, "profit_center_id")
    gen = pk_string(gen, "profit_center_nbr", "PC", 6)
    gen = gen.withColumn("profit_center_nm", T.StringType(),
                         baseColumn="profit_center_nbr",
                         expr="concat('Profit Center ', profit_center_nbr)")
    gen = enum_col(gen, "segment_id", ["1000", "2000", "3000", "4000", "5000"])
    gen = enum_col(gen, "geography_nm", geos, weights=[30, 25, 15, 10, 12, 8])
    gen = enum_col(gen, "profit_center_channel_nm", channels, weights=[30, 25, 20, 15, 10])
    gen = enum_col(gen, "territory_nm", territories)
    gen = enum_col(gen, "sub_territory_nm", ["North", "South", "East", "West", "Central"])
    gen = date_col(gen, "begin_effective_dt", begin="2010-01-01", end="2020-12-31")
    gen = date_col(gen, "end_effective_dt", begin="2025-01-01", end="2030-12-31")
    gen = active_ind_col(gen)
    gen = gen.withColumn("geography_sort", T.IntegerType(), minValue=1, maxValue=6, random=True)
    gen = enum_col(gen, "operating_segment_nm", segments)
    return gen.build()


# ---------------------------------------------------------------------------
# division_text
# ---------------------------------------------------------------------------

def gen_division_text(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    divisions = [
        (1, "01", "Footwear", "Core", "Y"),
        (2, "02", "Apparel", "Core", "Y"),
        (3, "03", "Equipment", "Core", "Y"),
        (4, "04", "Digital", "Digital", "Y"),
        (5, "05", "Golf", "Specialty", "Y"),
        (6, "06", "Jordan Brand", "Core", "Y"),
        (7, "07", "Converse", "Partner", "Y"),
        (8, "08", "Hurley", "Partner", "N"),
        (9, "09", "Nike SB", "Specialty", "Y"),
        (10, "10", "NikeLab", "Specialty", "Y"),
        (11, "11", "ACG", "Specialty", "Y"),
        (12, "12", "Nike Sport Research", "R&D", "Y"),
        (13, "13", "Nike Training", "Core", "Y"),
        (14, "14", "Nike Running", "Core", "Y"),
        (15, "15", "Nike Basketball", "Core", "Y"),
        (16, "16", "Nike Soccer", "Core", "Y"),
        (17, "17", "Nike Tennis", "Specialty", "Y"),
        (18, "18", "Nike Swim", "Specialty", "Y"),
        (19, "19", "Nike Yoga", "Core", "Y"),
        (20, "20", "Nike Kids", "Core", "Y"),
    ]
    import datetime
    records = []
    for i, (div_id, div_nbr, div_nm, div_group, active) in enumerate(divisions[:rows]):
        records.append((
            div_id, div_nbr, div_nm, div_group, "Y",
            f"view_{div_nbr}", "SAP_ECC",
            datetime.date(2020, 1, 1),
            datetime.date(2020, 1, 1),
            datetime.date(2023, 1, 1),
            "svc_datagen", "svc_datagen",
            "SAP_ECC", "EN", active,
        ))

    schema = T.StructType([
        T.StructField("division_id", T.LongType(), False),
        T.StructField("division_nbr", T.StringType(), True),
        T.StructField("division_nm", T.StringType(), True),
        T.StructField("division_group", T.StringType(), True),
        T.StructField("last_row_ind", T.StringType(), True),
        T.StructField("common_data_service_view_nm", T.StringType(), True),
        T.StructField("source_system_nm", T.StringType(), True),
        T.StructField("raw_tmst", T.DateType(), True),
        T.StructField("record_created_tmst_utc", T.DateType(), True),
        T.StructField("record_update_tmst_utc", T.DateType(), True),
        T.StructField("created_by_user_id", T.StringType(), True),
        T.StructField("updated_by_user_id", T.StringType(), True),
        T.StructField("physical_source_cd", T.StringType(), True),
        T.StructField("language_cd", T.StringType(), True),
        T.StructField("active_ind", T.StringType(), True),
    ])
    return spark.createDataFrame(records, schema)


# ---------------------------------------------------------------------------
# version_forecast_mapping
# ---------------------------------------------------------------------------

def gen_version_forecast_mapping(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "version_forecast_mapping", rows, partitions)
    gen = pk_bigint(gen, "version_forecast_mapping_id")
    gen = enum_col(gen, "version_nbr",
                   ["001", "002", "003", "010", "020", "100", "200", "BUD", "ACT", "FRC"])
    gen = enum_col(gen, "version_group_nm",
                   ["Actual", "Budget", "Forecast", "Rolling Forecast", "Plan", "Latest Estimate"],
                   weights=[40, 20, 20, 10, 7, 3])
    gen = active_ind_col(gen)
    return gen.build()


# ---------------------------------------------------------------------------
# functional_area
# ---------------------------------------------------------------------------

def gen_functional_area(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    func_areas = [
        "Sales", "Marketing", "Operations", "Finance", "HR", "R&D", "IT",
        "Legal", "Supply Chain", "Product Development", "Retail", "Digital",
        "Customer Service", "Engineering", "Manufacturing"
    ]
    gen = make_generator(spark, "functional_area", rows, partitions)
    gen = pk_bigint(gen, "functional_area_id")
    gen = enum_col(gen, "language_cd", ["EN", "DE", "FR", "ES", "ZH"], weights=[50, 15, 10, 10, 15])
    gen = pk_string(gen, "functional_area_cd", "FA", 4)
    gen = gen.withColumn("functional_area_nm", T.StringType(),
                         values=func_areas, random=True)
    return gen.build()


# ---------------------------------------------------------------------------
# finance_product_dim_v
# ---------------------------------------------------------------------------

def gen_finance_product_dim_v(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "finance_product_dim_v", rows, partitions)
    gen = pk_bigint(gen, "product_id")
    gen = enum_col(gen, "primary_platform_desc", ["Footwear", "Apparel", "Equipment", "Digital", "Accessories"],
                   weights=[40, 35, 10, 8, 7])
    gen = template_string(gen, "style_nm", r"rrrr dddd")
    gen = enum_col(gen, "franchise_nm",
                   ["Air Max", "Air Force", "Jordan", "React", "ZoomX", "Pegasus", "Vaporfly",
                    "Blazer", "Cortez", "Dunk", "Flyknit", "Free", "Metcon", "Revolution"],
                   weights=[15, 12, 15, 10, 8, 8, 5, 5, 5, 5, 4, 3, 3, 2])
    gen = enum_col(gen, "gender_desc",
                   ["Mens", "Womens", "Kids", "Unisex", "Grade School", "Toddler"],
                   weights=[35, 30, 15, 10, 7, 3])
    gen = enum_col(gen, "global_category_core_focus_desc",
                   ["Running", "Training & Gym", "Basketball", "Football", "Sportswear",
                    "Tennis", "Golf", "Yoga", "Swimming"],
                   weights=[25, 20, 15, 12, 12, 5, 4, 4, 3])
    gen = template_string(gen, "product_cd", r"rrr-ddddd-ddd")
    gen = enum_col(gen, "team_nm", ["NA", "Chicago Bulls", "LA Lakers", "Real Madrid", "None"],
                   weights=[60, 10, 10, 10, 10])
    gen = enum_col(gen, "league_desc", ["NBA", "NFL", "UEFA", "MLB", "NA"], weights=[20, 15, 15, 10, 40])
    gen = template_string(gen, "athlete_full_nm", r"rrrr rrrrrrr", nullable_pct=0.7)
    gen = enum_col(gen, "product_company_nm", ["Nike", "Jordan", "Converse", "Hurley"], weights=[65, 20, 10, 5])
    gen = enum_col(gen, "age_desc", ["Adult", "Kids", "Infant", "Toddler", "Youth"], weights=[55, 20, 8, 8, 9])
    gen = template_string(gen, "consumer_construct_dimension_nm", r"rrrrrrrrr")
    gen = template_string(gen, "fields_of_play_nm", r"rrrrrrr rrrrr")
    gen = enum_col(gen, "merchandising_classification_desc",
                   ["Core", "Launch", "Key Item", "Seasonal", "Clearance"], weights=[40, 20, 20, 15, 5])
    gen = enum_col(gen, "consumer_construct_segment_nm",
                   ["Performance Running", "Sportswear", "Jordan", "Training", "Kids"],
                   weights=[25, 30, 20, 15, 10])
    gen = enum_col(gen, "brand_nm", ["Nike", "Jordan", "Converse", "Hurley", "Nike Golf"],
                   weights=[65, 20, 10, 3, 2])
    gen = enum_col(gen, "sub_category_desc",
                   ["Shoes", "Tops", "Bottoms", "Outerwear", "Accessories", "Socks", "Bags"],
                   weights=[35, 20, 15, 10, 8, 7, 5])
    gen = enum_col(gen, "blank_usage_ind", ["Y", "N"], weights=[5, 95])
    gen = enum_col(gen, "silhouette_desc",
                   ["Low", "Mid", "High", "Slip-On", "Sandal", "Boot", "Crew"], weights=[40, 25, 15, 8, 5, 4, 3])
    gen = enum_col(gen, "silhouette_type_desc", ["Lace", "Strap", "Elastic", "Zip"], weights=[60, 20, 15, 5])
    gen = template_string(gen, "style_nbr", r"rr-dddddd-ddd")
    gen = template_string(gen, "consumer_construct_global_consumer_offense_nm", r"rrrrrrrrr rrrr")
    gen = active_ind_col(gen)
    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    gen = enum_col(gen, "global_sport_focus_derived_desc",
                   ["Running", "Training", "Basketball", "Football", "Sportswear"], weights=[25, 20, 20, 20, 15])
    gen = enum_col(gen, "global_sport_focus_desc",
                   ["Running", "Training", "Basketball", "Football", "Sportswear"], weights=[25, 20, 20, 20, 15])
    gen = template_string(gen, "global_sport_sub_focus_desc", r"rrrrrrrrr")
    gen = enum_col(gen, "sub_brand_desc",
                   ["Air Max", "React", "Zoom", "Free", "Pegasus", "Force", "Flight"],
                   weights=[20, 15, 15, 12, 12, 13, 13])
    gen = template_string(gen, "sub_brand_cd", r"rrd")
    return gen.build()


# ---------------------------------------------------------------------------
# finance_customer_dim_v
# ---------------------------------------------------------------------------

def gen_finance_customer_dim_v(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "finance_customer_dim_v", rows, partitions)
    gen = pk_bigint(gen, "finance_customer_id")
    gen = template_string(gen, "customer_nbr", r"dddddddd")
    gen = enum_col(gen, "channel_desc",
                   ["Wholesale", "Direct to Consumer", "Digital", "Own Stores", "Partner", "Factory Outlet"],
                   weights=[30, 25, 20, 10, 10, 5])
    gen = template_string(gen, "customer_nm", r"rrrrrrrr rrrrrrr")
    gen = template_string(gen, "customer_owner_group_nm", r"rrrrrrr rrrrrr")
    gen = enum_col(gen, "marketplace_channel_nm",
                   ["Nike Direct", "Wholesale", "Digital", "Factory Stores", "Nike App"], weights=[25, 30, 20, 15, 10])
    gen = enum_col(gen, "geo_marketplace_unit_nm",
                   ["North America", "EMEA", "Greater China", "APLA", "Europe"], weights=[30, 25, 15, 15, 15])
    gen = template_string(gen, "integrated_business_planning_level_1_desc", r"rrrrrr rrrrrr")
    gen = template_string(gen, "integrated_business_planning_level_2_desc", r"rrrrrr rrrrrr rrrr")
    gen = template_string(gen, "integrated_business_planning_level_3_desc", r"rrrrrr rrrrr rrrrrr")
    gen = template_string(gen, "integrated_business_planning_mpu_desc", r"rrrrrr rrrr")
    gen = template_string(gen, "sub_territory_nm", r"rrrrrrrr")
    gen = enum_col(gen, "customer_business_type_nm",
                   ["Specialty Athletic", "General Sporting Goods", "Department Store",
                    "Discount", "Digital Pure Player", "Mono Brand"], weights=[20, 20, 15, 15, 15, 15])
    gen = enum_col(gen, "customer_subtype_nm",
                   ["Key Account", "Mid-Tier", "Value", "Premium", "Franchise"], weights=[25, 25, 20, 20, 10])
    gen = enum_col(gen, "partner_channel",
                   ["Wholesale", "Digital", "DTC", "Franchise"], weights=[35, 30, 25, 10])
    gen = template_string(gen, "partner_sub_channel", r"rrrrrrrr")
    gen = enum_col(gen, "partner_account_classification",
                   ["Tier 1", "Tier 2", "Tier 3", "Strategic", "Global"], weights=[20, 25, 25, 20, 10])
    return gen.build()


# ---------------------------------------------------------------------------
# company_code
# ---------------------------------------------------------------------------

def gen_company_code(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "company_code", rows, partitions)
    gen = pk_bigint(gen, "company_id")
    gen = template_string(gen, "company_cd", r"dddd")
    gen = template_string(gen, "company_nm", r"rrrrrrrr rrrrrrrrrrr")
    gen = enum_col(gen, "currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY", "CHF", "CAD", "AUD"],
                   weights=[35, 20, 10, 8, 8, 5, 7, 7])
    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    return gen.build()


# ---------------------------------------------------------------------------
# copa_attribution_dim
# ---------------------------------------------------------------------------

def gen_copa_attribution_dim(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "copa_attribution_dim", rows, partitions)
    gen = pk_bigint(gen, "copa_attribution_id")
    gen = template_string(gen, "responsive_business_model_cd", r"rrd")
    gen = template_string(gen, "responsive_business_model_desc", r"rrrrrrrrr rrrrrrrrrr")
    gen = template_string(gen, "demand_stream_cd", r"ddd")
    gen = template_string(gen, "demand_stream_desc", r"rrrrrrr rrrrrr")
    gen = enum_col(gen, "business_type_cd", ["DTC", "WHK", "DIG", "MKT", "OWN"], weights=[25, 25, 20, 15, 15])
    gen = template_string(gen, "business_type_desc", r"rrrrrrrrrr rrrrrr")
    gen = template_string(gen, "marketing_type_cd", r"rrd")
    gen = template_string(gen, "marketing_type_desc", r"rrrrrrrr rrrr")
    gen = enum_col(gen, "gender_age_cd",
                   ["MN", "WN", "KD", "GS", "BG", "TD"], weights=[30, 25, 15, 12, 10, 8])
    gen = enum_col(gen, "gender_age_desc",
                   ["Mens", "Womens", "Kids", "Grade School", "Big Kids", "Toddler"],
                   weights=[30, 25, 15, 12, 10, 8])
    gen = template_string(gen, "direct_business_model_cd", r"rrd")
    gen = template_string(gen, "direct_business_model_desc", r"rrrrrrrrr rrrr")
    gen = enum_col(gen, "product_lifecycle_cd", ["INIT", "GROW", "MATT", "DECL"], weights=[20, 30, 35, 15])
    gen = template_string(gen, "product_lifecycle_desc", r"rrrrrrrrrrr")
    gen = enum_col(gen, "quality_cd", ["PRM", "STD", "VAL"], weights=[30, 50, 20])
    gen = template_string(gen, "quality_desc", r"rrrrrrrrrr")
    gen = template_string(gen, "region_summary_product_group_cd", r"rrd")
    gen = template_string(gen, "region_summary_product_group_desc", r"rrrrrr rrrrrrr")
    gen = template_string(gen, "sales_order_reason_desc", r"rrrrrrrrr rrrrrr")
    gen = enum_col(gen, "sales_order_type_cd",
                   ["OR", "ZOR", "ZDR", "RE", "ZRE", "CS", "ZCS"], weights=[30, 20, 10, 10, 8, 12, 10])
    gen = template_string(gen, "sales_order_type_desc", r"rrrrrrrr rrrrr rrrr")
    gen = template_string(gen, "sales_order_type_group_desc", r"rrrrrrrrrr rrrr")
    gen = template_string(gen, "sales_order_item_category_cd", r"rrrr")
    gen = template_string(gen, "sales_order_item_category_desc", r"rrrrrrrrr rrrrrr rrrrr")
    gen = enum_col(gen, "distribution_method_cd",
                   ["WHK", "DTC", "DIGITAL", "WHL", "MKT", "OWNED"], weights=[25, 20, 20, 15, 10, 10])
    gen = template_string(gen, "distribution_method_desc", r"rrrrrrrrrrrr rrrrrrr")
    gen = template_string(gen, "sales_order_reason_cd", r"rrd")
    return gen.build()


# ---------------------------------------------------------------------------
# cost_center_dim_v  (string PK)
# ---------------------------------------------------------------------------

def gen_cost_center_dim_v(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    """
    cost_center_nbr is the string PK used as FK from general_ledger_fact.
    Format: CC_000001 … CC_005000
    The fact generator must use fk_string('cost_center_nbr', 'CC', ref_rows, 6).
    """
    gen = make_generator(spark, "cost_center_dim_v", rows, partitions)
    gen = pk_string(gen, "cost_center_nbr", "CC", 6)
    gen = enum_col(gen, "controlling_area_cd",
                   ["NA01", "EU01", "AP01", "CA01", "LA01", "GC01"], weights=[30, 25, 15, 10, 10, 10])
    gen = date_col(gen, "valid_to_dt", begin="2025-01-01", end="2035-12-31")
    gen = date_col(gen, "valid_from_dt", begin="2000-01-01", end="2020-12-31")
    gen = enum_col(gen, "iso_language_cd", ["EN", "DE", "FR", "ES", "ZH"], weights=[50, 15, 10, 10, 15])
    gen = gen.withColumn("cost_center_nm", T.StringType(),
                         baseColumn="cost_center_nbr",
                         expr="concat('Cost Center ', cost_center_nbr)")
    gen = gen.withColumn("cost_center_desc", T.StringType(),
                         baseColumn="cost_center_nm",
                         expr="concat('Description for ', cost_center_nm)")
    gen = template_string(gen, "cost_center_category_hierarchy_1_cd", r"rrd")
    gen = template_string(gen, "cost_center_category_hierarchy_2_cd", r"rrd")
    gen = template_string(gen, "company_cd", r"dddd")
    gen = enum_col(gen, "source_system", ["SAP_ECC", "SAP_S4", "BW"], weights=[40, 40, 20])
    gen = enum_col(gen, "cost_center_type_cd", ["K", "A", "F", "L", "M"], weights=[50, 20, 15, 10, 5])
    gen = template_string(gen, "cost_center_category_short_desc", r"rrrrrrrrr")
    gen = template_string(gen, "business_area_cd", r"dddd")
    gen = template_string(gen, "tax_jurisdiction_cd", r"rr-ddd-ddd")
    gen = template_string(gen, "functional_area_cd", r"FA_dddd")
    gen = enum_col(gen, "currency_cd",
                   ["USD", "EUR", "GBP", "JPY", "CNY", "CHF", "CAD"],
                   weights=[35, 20, 10, 8, 10, 7, 10])
    for ind_col in ["posting_allowed_ind", "planning_allowed_ind",
                    "secondary_costs_posting_allowed_ind", "revenue_posting_allowed_ind",
                    "commitment_update_allowed_ind", "secondary_costs_planning_allowed_ind",
                    "revenue_planning_allowed_ind", "quantity_required_ind"]:
        gen = active_ind_col(gen, ind_col, active_pct=80)
    gen = template_string(gen, "department_nm", r"rrrrrrrr rrrrrrrrrrr")
    gen = template_string(gen, "cost_center_report_printer_destination_cd", r"rrrrd")
    gen = template_string(gen, "company_legal_entity_id", r"rrd_dddddd")
    gen = template_string(gen, "profit_center_nbr", r"PC_dddddd")
    gen = template_string(gen, "responsible_user_nm", r"rrrrrrrr rrrrrrr")
    gen = user_id_col(gen, "responsible_user_id")
    gen = template_string(gen, "responsible_user_title", r"rrrrrrrrr")
    for line_col in ["line_1_nm", "line_2_nm", "line_3_nm", "line_4_nm"]:
        gen = template_string(gen, line_col, r"rrrrrrrrrr rrrrrr", nullable_pct=0.5)
    gen = enum_col(gen, "country_cd",
                   ["US", "DE", "GB", "FR", "CN", "JP", "KR", "AU", "CA"],
                   weights=[30, 10, 8, 8, 10, 5, 3, 3, 5])
    gen = template_string(gen, "region_cd", r"rrd")
    gen = template_string(gen, "city_nm", r"rrrrrrrrr")
    gen = template_string(gen, "district_nm", r"rrrrrrr rrrrrr", nullable_pct=0.3)
    gen = template_string(gen, "postal_cd", r"ddddd")
    gen = template_string(gen, "street_address_txt", r"ddd rrrrrrr rrrrrr")
    gen = template_string(gen, "po_box_postal_cd", r"ddddd", nullable_pct=0.6)
    gen = template_string(gen, "po_box_nbr", r"ddddd", nullable_pct=0.6)
    gen = enum_col(gen, "correspondence_language_cd", ["EN", "DE", "FR", "ES", "ZH"], weights=[50, 15, 10, 10, 15])
    gen = template_string(gen, "first_telephone_nbr", r"+d (ddd) ddd-dddd")
    gen = template_string(gen, "second_telephone_nbr", r"+d (ddd) ddd-dddd", nullable_pct=0.7)
    for comm_col in ["telebox_nbr", "fax_nbr", "teletex_nbr", "telex_nbr", "data_communication_line_nbr"]:
        gen = template_string(gen, comm_col, r"ddddddddd", nullable_pct=0.9)
    gen = date_col(gen, "msg_header_tmst", nullable_pct=0.3)
    gen = date_col(gen, "begin_effective_dt", begin="2000-01-01", end="2020-12-31")
    gen = date_col(gen, "end_effective_dt", begin="2025-01-01", end="2035-12-31")
    gen = gen.withColumn(
        "cost_center_id", T.LongType(),
        baseColumn="__cost_center_nbr_id",
        expr="__cost_center_nbr_id",
    )
    gen = date_col(gen, "_cost_center_cleansed_latest_load_timestamp", begin="2023-01-01", end="2025-12-31")
    return gen.build()


# ---------------------------------------------------------------------------
# geo_wholesale_value_business_dim
# ---------------------------------------------------------------------------

def gen_geo_wholesale_value_business_dim(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    gen = make_generator(spark, "geo_wholesale_value_business_dim", rows, partitions)
    gen = pk_bigint(gen, "geo_wholesale_value_business_id")
    gen = template_string(gen, "geo_wholesale_value_business_desc", r"rrrrrrrrr rrrrrrrr rrrr")
    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    return gen.build()


# ---------------------------------------------------------------------------
# geo_marketplace_channel_dim
# ---------------------------------------------------------------------------

def gen_geo_marketplace_channel_dim(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    channels = ["Nike Direct - NA", "Nike Direct - EMEA", "Nike Direct - APLA",
                "Nike Direct - GC", "Wholesale - NA", "Wholesale - EMEA",
                "Wholesale - APLA", "Digital - NA", "Digital - EMEA", "Digital - GC"]
    gen = make_generator(spark, "geo_marketplace_channel_dim", rows, partitions)
    gen = pk_bigint(gen, "geo_marketplace_channel_id")
    gen = gen.withColumn("geo_marketplace_channel_nm", T.StringType(),
                         values=channels * (rows // len(channels) + 1), random=True)
    gen = user_id_col(gen, "created_by_user_id")
    gen = user_id_col(gen, "updated_by_user_id")
    gen = physical_source_col(gen)
    gen = active_ind_col(gen)
    return gen.build()


# ---------------------------------------------------------------------------
# gl_account_dim  (string PK)
# ---------------------------------------------------------------------------

def gen_gl_account_dim(spark: SparkSession, rows: int, partitions: int) -> DataFrame:
    """
    gl_account_nbr is the string PK.
    Format: GL_000001 … GL_002000
    Fact generator must use fk_string('gl_account_nbr', 'GL', ref_rows, 6).
    """
    gen = make_generator(spark, "gl_account_dim", rows, partitions)
    gen = pk_string(gen, "gl_account_nbr", "GL", 6)
    gen = gen.withColumn("gl_account_short_desc", T.StringType(),
                         baseColumn="gl_account_nbr",
                         expr="concat('GL Account ', gl_account_nbr)")
    gen = gen.withColumn("gl_account_long_desc", T.StringType(),
                         baseColumn="gl_account_short_desc",
                         expr="concat('General Ledger Account: ', gl_account_short_desc)")
    gen = date_col(gen, "begin_effective_dt", begin="2000-01-01", end="2018-12-31")
    gen = date_col(gen, "end_effective_dt", begin="2025-01-01", end="2035-12-31")
    gen = active_ind_col(gen)
    gen = gen.withColumn(
        "gl_accnt_id", T.LongType(),
        baseColumn="__gl_account_nbr_id",
        expr="__gl_account_nbr_id",
    )
    gen = enum_col(gen, "cost_component_calc", ["Y", "N", ""], weights=[30, 30, 40])
    return gen.build()
