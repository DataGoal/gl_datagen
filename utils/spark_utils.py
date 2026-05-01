"""
utils/spark_utils.py
--------------------
Spark session helpers and Delta write utilities.
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def get_or_create_spark(app_name: str = "GL_DataGen") -> SparkSession:
    """
    Return the active SparkSession (Databricks already has one) or create a
    local session for unit testing.
    """
    try:
        # In Databricks the session already exists — just get it.
        return SparkSession.builder.getOrCreate()
    except Exception:
        return (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )


def configure_spark(spark: SparkSession, shuffle_partitions: int = 400) -> None:
    """Apply performance tuning settings before generating large tables."""
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")


def write_table(
    df: DataFrame,
    full_table_name: str,
    write_mode: str = "overwrite",
    partition_by: Optional[list] = None,
) -> None:
    """
    Write a DataFrame to a Unity Catalog Delta table.

    Parameters
    ----------
    df              : Spark DataFrame to persist.
    full_table_name : Three-part name  catalog.schema.table.
    write_mode      : 'overwrite' or 'append'.
    partition_by    : Optional list of column names to partition the Delta table.
    """
    writer = df.write.format("delta").mode(write_mode).option(
        "overwriteSchema", "true"
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(full_table_name)
    print(f"[DataGen] Written {full_table_name} (mode={write_mode})")


def ensure_catalog_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the catalog and schema if they don't already exist."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"[DataGen] Ensured catalog={catalog} schema={schema}")
