"""
utils/datagen_helpers.py
------------------------
Reusable helpers that wrap dbldatagen's DataGenerator API.
Keeps generator code concise and consistent across all tables.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import dbldatagen as dg
from pyspark.sql import SparkSession
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# Type-mapping helpers
# ---------------------------------------------------------------------------

def spark_type(data_type: str) -> T.DataType:
    """Map a schema.yaml data_type string to a PySpark DataType."""
    dt = data_type.lower().strip()
    if dt in ("bigint", "long"):
        return T.LongType()
    if dt == "int" or dt == "integer":
        return T.IntegerType()
    if dt.startswith("decimal"):
        # e.g. decimal(18,5) → DecimalType(18,5)
        inner = dt.replace("decimal", "").strip("()")
        parts = [p.strip() for p in inner.split(",")]
        precision = int(parts[0]) if parts[0] else 18
        scale = int(parts[1]) if len(parts) > 1 and parts[1] else 5
        return T.DecimalType(precision, scale)
    if dt == "float":
        return T.FloatType()
    if dt == "double":
        return T.DoubleType()
    if dt == "date":
        return T.DateType()
    if dt == "boolean":
        return T.BooleanType()
    # string / varchar(n) → StringType
    return T.StringType()


# ---------------------------------------------------------------------------
# DataGenerator factory
# ---------------------------------------------------------------------------

def make_generator(
    spark: SparkSession,
    name: str,
    rows: int,
    partitions: int,
    seed: int = 42,
) -> dg.DataGenerator:
    """Return a DataGenerator instance with common defaults."""
    return (
        dg.DataGenerator(
            spark,
            name=name,
            rows=rows,
            partitions=partitions,
            randomSeedMethod="hash_fieldname",
            random=True,
            seed=seed,
        )
    )


# ---------------------------------------------------------------------------
# Column-spec builders
# ---------------------------------------------------------------------------

def pk_bigint(gen: dg.DataGenerator, col_name: str) -> dg.DataGenerator:
    """Sequential unique bigint PK starting from 1."""
    return gen.withColumn(col_name, T.LongType(), minValue=1, maxValue=gen.rowCount,
                          uniqueValues=gen.rowCount, random=False)


def pk_int(gen: dg.DataGenerator, col_name: str) -> dg.DataGenerator:
    """Sequential unique int PK starting from 1."""
    return gen.withColumn(col_name, T.IntegerType(), minValue=1, maxValue=gen.rowCount,
                          uniqueValues=gen.rowCount, random=False)


def pk_string(
    gen: dg.DataGenerator,
    col_name: str,
    prefix: str,
    zero_pad: int = 6,
    keep_id_col: bool = False,
) -> dg.DataGenerator:
    """
    String PK derived from a sequential counter.
    e.g. prefix='CC', zero_pad=6  ->  'CC_000001' ... 'CC_005000'

    A hidden bigint counter `__{col_name}_id` is created to back the string.
    By default it is OMITTED from the output. Set ``keep_id_col=True`` to
    expose it (used when the schema explicitly contains the id column).
    """
    id_col = f"__{col_name}_id"
    gen = gen.withColumn(
        id_col, T.LongType(),
        minValue=1, maxValue=gen.rowCount,
        uniqueValues=gen.rowCount, random=False,
        omit=not keep_id_col,
    )
    return gen.withColumn(
        col_name, T.StringType(),
        baseColumn=id_col,
        expr=f"concat('{prefix}_', lpad(string({id_col}), {zero_pad}, '0'))",
    )


def fk_bigint(
    gen: dg.DataGenerator,
    col_name: str,
    ref_rows: int,
    nullable: bool = False,
) -> dg.DataGenerator:
    """Random FK referencing a dim table that has sequential PK [1..ref_rows]."""
    if ref_rows is None or ref_rows < 1:
        ref_rows = 1
    kwargs: Dict[str, Any] = dict(minValue=1, maxValue=ref_rows, random=True)
    if nullable:
        kwargs["percentNulls"] = 0.02
    return gen.withColumn(col_name, T.LongType(), **kwargs)


def fk_int(
    gen: dg.DataGenerator,
    col_name: str,
    ref_rows: int,
) -> dg.DataGenerator:
    """Random int FK referencing a dim with sequential int PK [1..ref_rows]."""
    if ref_rows is None or ref_rows < 1:
        ref_rows = 1
    return gen.withColumn(col_name, T.IntegerType(), minValue=1, maxValue=ref_rows,
                          random=True)


def fk_string(
    gen: dg.DataGenerator,
    col_name: str,
    prefix: str,
    ref_rows: int,
    zero_pad: int = 6,
    keep_id_col: bool = False,
) -> dg.DataGenerator:
    """
    String FK that matches the ``pk_string`` format used in the referenced dim.
    Uses the same prefix + zero_pad convention.

    By default the internal bigint counter column ``__{col_name}_fk_id`` is
    omitted from the output. Set ``keep_id_col=True`` to expose it as a real
    table column (used when the DDL explicitly declares the id column).
    """
    if ref_rows is None or ref_rows < 1:
        ref_rows = 1
    tmp_col = f"__{col_name}_fk_id"
    gen = gen.withColumn(
        tmp_col, T.LongType(),
        minValue=1, maxValue=ref_rows,
        random=True, omit=not keep_id_col,
    )
    return gen.withColumn(
        col_name, T.StringType(),
        baseColumn=tmp_col,
        expr=f"concat('{prefix}_', lpad(string({tmp_col}), {zero_pad}, '0'))",
    )


# ---------------------------------------------------------------------------
# Calendar / fiscal-period helpers
# ---------------------------------------------------------------------------

# Maximum span the calendar dim is allowed to cover. 16 years * 12 = 192 periods.
# `gen_calendar_fiscal_period_v` and `fiscal_year_period` both honour this cap so
# fact FK values can never escape the dim's PK domain.
FISCAL_START_YEAR: int = 2018
FISCAL_MAX_YEARS: int = 16
FISCAL_MAX_PERIODS: int = FISCAL_MAX_YEARS * 12


def fiscal_period_values(
    count: int,
    start_year: int = FISCAL_START_YEAR,
    n_years: int = FISCAL_MAX_YEARS,
) -> List[int]:
    """
    Return the first ``count`` valid YYYYPP combos starting at ``start_year``.

    The set is capped at ``n_years * 12`` so callers get a deterministic,
    well-bounded domain that the calendar dim and every fact FK share.
    """
    all_vals = [yr * 100 + mo
                for yr in range(start_year, start_year + n_years)
                for mo in range(1, 13)]
    if count is None or count < 1:
        return all_vals
    return all_vals[:min(count, len(all_vals))]


def fiscal_year_period(
    gen: dg.DataGenerator,
    col_name: str,
    count: int = FISCAL_MAX_PERIODS,
    start_year: int = FISCAL_START_YEAR,
) -> dg.DataGenerator:
    """
    Random ``fiscal_year_period_nbr`` (YYYYPP) drawn from the same first-N list
    that the calendar dim materialises. Pass ``count`` from
    ``dim_rows['calendar_fiscal_period_v']`` to keep facts and dim aligned.
    """
    return gen.withColumn(
        col_name, T.IntegerType(),
        values=fiscal_period_values(count, start_year),
        random=True,
    )


def _clamp_pct(pct: float) -> float:
    """Clamp a null-probability into [0.0, 1.0]; dbldatagen rejects values outside that range."""
    if pct is None:
        return 0.0
    if pct < 0.0:
        return 0.0
    if pct > 1.0:
        return 1.0
    return float(pct)


def enum_col(
    gen: dg.DataGenerator,
    col_name: str,
    values: List[Any],
    weights: Optional[List[int]] = None,
    nullable_pct: float = 0.0,
) -> dg.DataGenerator:
    """Random selection from a fixed value list with optional weights."""
    kwargs: Dict[str, Any] = dict(values=values, random=True)
    if weights:
        kwargs["weights"] = weights
    nullable_pct = _clamp_pct(nullable_pct)
    if nullable_pct > 0:
        kwargs["percentNulls"] = nullable_pct
    return gen.withColumn(col_name, T.StringType(), **kwargs)


def template_string(
    gen: dg.DataGenerator,
    col_name: str,
    template: str,
    nullable_pct: float = 0.0,
) -> dg.DataGenerator:
    """
    String column generated from a dbldatagen template.
    r = random uppercase, d = digit, a = random lowercase.
    e.g. 'r_ddd' → 'A_143'
    """
    kwargs: Dict[str, Any] = dict(template=template, random=True)
    nullable_pct = _clamp_pct(nullable_pct)
    if nullable_pct > 0:
        kwargs["percentNulls"] = nullable_pct
    return gen.withColumn(col_name, T.StringType(), **kwargs)


def decimal_amount(
    gen: dg.DataGenerator,
    col_name: str,
    min_val: float = -1_000_000.0,
    max_val: float = 5_000_000.0,
    precision: int = 28,
    scale: int = 5,
    nullable_pct: float = 0.0,
) -> dg.DataGenerator:
    """Random decimal amount column."""
    kwargs: Dict[str, Any] = dict(minValue=min_val, maxValue=max_val, random=True)
    nullable_pct = _clamp_pct(nullable_pct)
    if nullable_pct > 0:
        kwargs["percentNulls"] = nullable_pct
    return gen.withColumn(col_name, T.DecimalType(precision, scale), **kwargs)


def date_col(
    gen: dg.DataGenerator,
    col_name: str,
    begin: str = "2015-01-01",
    end: str = "2025-12-31",
    nullable_pct: float = 0.0,
) -> dg.DataGenerator:
    """Random date column within a range."""
    kwargs: Dict[str, Any] = dict(begin=begin, end=end, random=True)
    nullable_pct = _clamp_pct(nullable_pct)
    if nullable_pct > 0:
        kwargs["percentNulls"] = nullable_pct
    return gen.withColumn(col_name, T.DateType(), **kwargs)


def active_ind_col(
    gen: dg.DataGenerator,
    col_name: str = "active_ind",
    active_pct: int = 90,
) -> dg.DataGenerator:
    """Y/N active indicator with weighted distribution."""
    return gen.withColumn(
        col_name, T.StringType(),
        values=["Y", "N"],
        weights=[active_pct, 100 - active_pct],
        random=True,
    )


def user_id_col(gen: dg.DataGenerator, col_name: str) -> dg.DataGenerator:
    """Realistic user/employee ID string."""
    return template_string(gen, col_name, template=r"addrr_ddddd", nullable_pct=0.01)


def physical_source_col(gen: dg.DataGenerator, col_name: str = "physical_source_cd") -> dg.DataGenerator:
    return enum_col(
        gen, col_name,
        values=["SAP_ECC", "SAP_S4", "BW", "HANA", "LEGACY", "ANAPLAN"],
        weights=[35, 30, 15, 10, 5, 5],
    )
