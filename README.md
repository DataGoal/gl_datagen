# GL DataGen — Databricks Synthetic Data Generator

A production-quality, configuration-driven data generator built on
[dbldatagen](https://databrickslabs.github.io/dbldatagen/) for the
`dev_pbi_perform_cf_poc_2B_agg` schema. Generates realistic, referentially
consistent data at any scale — from 1 million to 25 billion rows.

---

## Project Structure

```
gl_datagen/
├── configs/
│   ├── data_volumes.yaml        ← Row counts, partitions, write mode
│   ├── generation_config.yaml   ← Domain value lists (currencies, channels, etc.)
│   └── schema.yaml              ← Source schema definition (PK/FK relationships)
│
├── src/
│   ├── orchestrator.py          ← Main entry point; reads config and drives generation
│   └── generators/
│       ├── dimension_generators.py   ← All FK-referenced dimension tables
│       ├── fact_generators.py        ← general_ledger_fact, CIS_fact, CBS_fact
│       └── hierarchy_generators.py   ← All hierarchy and reference tables
│
├── utils/
│   ├── schema_parser.py         ← Parses schema.yaml into typed dataclasses
│   ├── spark_utils.py           ← Session helpers, Delta write, catalog/schema setup
│   └── datagen_helpers.py       ← Reusable dbldatagen column-spec builders
│
├── notebooks/
│   └── 01_run_datagen.py        ← Databricks notebook entry point
│
└── README.md
```

---

## Quick Start

### 1. Upload to Databricks Repos

```bash
git clone <this-repo>
# Push to your Git provider, then sync via Databricks Repos UI
```

### 2. Install dependencies

```python
%pip install dbldatagen pyyaml
```

### 3. Open and run the notebook

Open `notebooks/01_run_datagen.py` in Databricks and click **Run All**.

---

## Scaling to 25 Billion Rows

Change one line in `configs/data_volumes.yaml`:

```yaml
general_ledger_fact:
  rows: 25_000_000_000    # ← was 10_000_000
  partitions: 2000
  shuffle_partitions: 2000
```

Or override at runtime without editing YAML:

```python
from src.orchestrator import DataGenOrchestrator

orch = DataGenOrchestrator()
orch.run(
    tables=["general_ledger_fact"],
    overrides={
        "general_ledger_fact": {
            "rows": 25_000_000_000,
            "partitions": 2000,
        }
    }
)
```

### Recommended cluster for 25B rows

| Component | Specification |
|-----------|--------------|
| Driver | 64 GB RAM, 16 cores |
| Workers | 16–32 × Standard_DS4_v2 (28 GB, 8 cores) |
| Runtime | Databricks 14.x + Delta 3.x |
| Partitions | 2000 (≈12.5M rows/partition) |

---

## How Referential Integrity Works

All FK-referenced dimensions generate PKs as **sequential ranges `[1..rows]`**.

Fact table FK columns use `minValue=1, maxValue=dim_rows, random=True` in dbldatagen.
This guarantees every FK value maps to an existing dimension PK without broadcasting
or joining — which would be impossible at 25B rows.

String PK dimensions (`cost_center_dim_v`, `gl_account_dim`) use a deterministic
template (e.g. `CC_000001`…`CC_005000`). The fact generator reconstructs the same
template from a random integer seed in `[1..dim_rows]`.

---

## Table Generation Order

```
Phase 1 — Dimensions (no dependencies)
  accounting_document_type
  calendar_fiscal_period_v      ← Real fiscal periods YYYYPP
  profit_center
  division_text                 ← Fixed 20-row seed table
  version_forecast_mapping
  functional_area
  finance_product_dim_v
  finance_customer_dim_v
  company_code
  copa_attribution_dim
  cost_center_dim_v             ← String PK: CC_000001
  geo_wholesale_value_business_dim
  geo_marketplace_channel_dim
  gl_account_dim                ← String PK: GL_000001

Phase 2 — Fact tables (reference dimensions above)
  general_ledger_fact           ← Central fact, 14 FKs
  CIS_fact
  consolidated_balance_sheet_fact

Phase 3 — Hierarchy & reference (no FK constraints)
  atscale_geo_security
  consolidation_functional_area_hierarchy
  consolidation_segment_hierarchy_dim
  segment_cost_center_hierarchy_dim_v
  segment_profit_center_hierarchy
  DisChannel_cost_center_hierarchy_dim_v
  DisChannel_profit_center_hierarchy
  PartDisChannel_profit_center_hierarchy
  division_text_dim_v
  gl_account_hierarchy
  management_gl_account_hierarchy
  gl_account_zfsm_measures_hierarchy_dim
  finance_foreign_currency_exchange_rate
  retail_global_store_profile_v
```

---

## Generating Only Selected Tables

```python
orch.run(tables=["general_ledger_fact", "profit_center"])
```

---

## Customising Value Domains

All string domain value lists live in `configs/generation_config.yaml`.
Edit the lists and weights there — no Python changes required.

---

## Data Quality Characteristics

| Characteristic | Approach |
|---------------|----------|
| Fiscal periods | Real YYYYPP calendar (2018–2026) |
| Amounts | Signed decimals with realistic ranges (–$5M … +$10M) |
| FK integrity | Sequential dim PKs + bounded random fact FKs |
| String FKs | Deterministic prefix+pad template shared across dim and fact |
| Indicators | Weighted Y/N (e.g. active_ind: 90% Y, 10% N) |
| Currencies | Weighted realistic distribution (USD 35%, EUR 20%, etc.) |
| Nulls | `percentNulls` applied only to nullable columns |
| Dates | Domain-appropriate ranges (e.g. valid_from < valid_to) |
