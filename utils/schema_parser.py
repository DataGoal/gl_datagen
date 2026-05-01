"""
utils/schema_parser.py
----------------------
Parses schema.yaml into strongly-typed dataclasses consumed by generators.
"""
from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ColumnRelationship:
    column: str
    ref_table: str
    ref_column: str


@dataclass
class ColumnDef:
    name: str
    data_type: str
    constraints: List[str] = field(default_factory=list)

    @property
    def is_pk(self) -> bool:
        return "primary_key" in self.constraints

    @property
    def is_not_null(self) -> bool:
        return "not_null" in self.constraints

    @property
    def base_type(self) -> str:
        """Return the base SQL type without precision qualifiers."""
        dt = self.data_type.lower()
        if dt.startswith("varchar"):
            return "string"
        if dt.startswith("decimal"):
            return "decimal"
        return dt


@dataclass
class TableDef:
    name: str
    columns: List[ColumnDef]
    relationships: List[ColumnRelationship] = field(default_factory=list)
    description: Optional[str] = None

    def pk_column(self) -> Optional[ColumnDef]:
        for col in self.columns:
            if col.is_pk:
                return col
        return None

    def get_column(self, name: str) -> Optional[ColumnDef]:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def fk_map(self) -> Dict[str, ColumnRelationship]:
        """Return {column_name: relationship} for all FK columns."""
        return {rel.column: rel for rel in self.relationships}


class SchemaParser:
    """Loads and exposes the full schema from schema.yaml."""

    def __init__(self, schema_path: str):
        with open(schema_path, "r") as fh:
            raw = yaml.safe_load(fh)

        self.catalog: str = raw.get("catalog", "development")
        self.schema: str = raw.get("schema", "default")
        self._tables: Dict[str, TableDef] = {}

        for tbl_raw in raw.get("tables", []):
            cols = [
                ColumnDef(
                    name=c["name"],
                    data_type=c["data_type"],
                    constraints=c.get("constraints", []),
                )
                for c in tbl_raw.get("columns", [])
            ]
            rels = [
                ColumnRelationship(
                    column=r["column"],
                    ref_table=r["references"]["table"],
                    ref_column=r["references"]["column"],
                )
                for r in tbl_raw.get("relationships", [])
            ]
            self._tables[tbl_raw["name"]] = TableDef(
                name=tbl_raw["name"],
                columns=cols,
                relationships=rels,
                description=tbl_raw.get("description"),
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def tables(self) -> Dict[str, TableDef]:
        return self._tables

    def get_table(self, name: str) -> TableDef:
        tbl = self._tables.get(name)
        if tbl is None:
            raise KeyError(f"Table '{name}' not found in schema.")
        return tbl

    def full_name(self, table_name: str) -> str:
        """Return the Unity Catalog three-part table name."""
        return f"{self.catalog}.{self.schema}.{table_name}"

    def pk_column_name(self, table_name: str) -> Optional[str]:
        tbl = self._tables.get(table_name)
        if not tbl:
            return None
        pk = tbl.pk_column()
        return pk.name if pk else None
