CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.calendar_fiscal_period_v (
  fiscal_year_period_nbr INT NOT NULL,
  month_long_nm STRING,
  month_short_nm STRING,
  month_nbr INT,
  year_mth INT,
  month_relevance_dt DATE,
  month_start_dt DATE,
  month_end_dt DATE,
  month_sort_sequence_nbr INT,
  fiscal_period_nbr INT,
  fiscal_period_cd STRING,
  fiscal_period_sort_sequence_nbr INT,
  fiscal_year_period_cd STRING,
  fiscal_year_period_nm STRING,
  season_period_cd STRING,
  season_alternate_period_cd STRING,
  season_nm STRING,
  season_relevance_dt DATE,
  season_start_dt DATE,
  season_end_dt DATE,
  season_sort_sequence_nbr INT,
  quarter_calendar_nbr INT,
  quarter_calendar_sequence_nbr INT,
  quarter_business_nbr INT,
  fiscal_quarter_nbr INT,
  fiscal_quarter_cd STRING,
  fiscal_quarter_sort_sequence_nbr INT,
  fiscal_year_quarter_nbr INT,
  fiscal_year_quarter_cd STRING,
  fiscal_year_quarter_alternate_cd STRING,
  year_cd STRING,
  year_nm STRING,
  year_nbr STRING,
  year_start_dt DATE,
  year_end_dt DATE,
  business_year_nbr INT,
  fiscal_year_nbr INT,
  fiscal_year_cd STRING,
  fiscal_period_sort INT,
  CONSTRAINT pk_calendar_fiscal_period_v PRIMARY KEY (fiscal_year_period_nbr))
USING delta
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd');

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.segment_cost_center_hierarchy_dim_v (
  cost_center_hierarchy_hist_id BIGINT,
  cost_center_nbr STRING,
  cost_center_hierarchy_nm STRING,
  cost_center_level_1_cd STRING,
  cost_center_level_1_nm STRING,
  cost_center_level_2_cd STRING,
  cost_center_level_2_nm STRING,
  cost_center_level_3_cd STRING,
  cost_center_level_3_nm STRING,
  cost_center_level_4_cd STRING,
  cost_center_level_4_nm STRING,
  cost_center_level_5_cd STRING,
  cost_center_level_5_nm STRING,
  cost_center_level_6_cd STRING,
  cost_center_level_6_nm STRING,
  cost_center_level_7_cd STRING,
  cost_center_level_7_nm STRING,
  cost_center_level_8_cd STRING,
  cost_center_level_8_nm STRING,
  cost_center_level_9_cd STRING,
  cost_center_level_9_nm STRING,
  cost_center_level_10_cd STRING,
  cost_center_level_10_nm STRING,
  cost_center_level_11_cd STRING,
  cost_center_level_11_nm STRING,
  cost_center_level_12_cd STRING,
  cost_center_level_12_nm STRING,
  cost_center_level_13_cd STRING,
  cost_center_level_13_nm STRING,
  cost_center_level_14_cd STRING,
  cost_center_level_14_nm STRING,
  cost_center_level_15_cd STRING,
  cost_center_level_15_nm STRING,
  cost_center_level_16_cd STRING,
  cost_center_level_16_nm STRING,
  cost_center_level_17_cd STRING,
  cost_center_level_17_nm STRING,
  cost_center_level_18_cd STRING,
  cost_center_level_18_nm STRING,
  cost_center_level_19_cd STRING,
  cost_center_level_19_nm STRING,
  cost_center_level_20_cd STRING,
  cost_center_level_20_nm STRING,
  cost_center_level_21_cd STRING,
  cost_center_level_21_nm STRING,
  cost_center_level_22_cd STRING,
  cost_center_level_22_nm STRING,
  cost_center_level_23_cd STRING,
  cost_center_level_23_nm STRING,
  cost_center_level_24_cd STRING,
  cost_center_level_24_nm STRING,
  cost_center_level_25_cd STRING,
  cost_center_level_25_nm STRING,
  cost_center_level_26_cd STRING,
  cost_center_level_26_nm STRING,
  cost_center_level_27_cd STRING,
  cost_center_level_27_nm STRING,
  cost_center_level_28_cd STRING,
  cost_center_level_28_nm STRING,
  cost_center_level_29_cd STRING,
  cost_center_level_29_nm STRING,
  cost_center_level_30_cd STRING,
  cost_center_level_30_nm STRING,
  controlling_area_cd STRING)
USING delta
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd');

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.segment_profit_center_hierarchy (
  __segment_profit_center_nbr_id BIGINT,
  profit_center_nbr STRING,
  profit_center_hierarchy_nm STRING,
  profit_center_level_1_cd STRING,
  profit_center_level_1_nm STRING,
  profit_center_level_2_cd STRING,
  profit_center_level_2_nm STRING,
  profit_center_level_3_cd STRING,
  profit_center_level_3_nm STRING,
  profit_center_level_4_cd STRING,
  profit_center_level_4_nm STRING,
  profit_center_level_5_cd STRING,
  profit_center_level_5_nm STRING,
  profit_center_level_6_cd STRING,
  profit_center_level_6_nm STRING,
  profit_center_level_7_cd STRING,
  profit_center_level_7_nm STRING,
  profit_center_level_8_cd STRING,
  profit_center_level_8_nm STRING,
  profit_center_level_9_cd STRING,
  profit_center_level_9_nm STRING)
USING delta
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd');

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.version_forecast_mapping (
  version_forecast_mapping_id BIGINT NOT NULL,
  version_nbr STRING,
  version_group_nm STRING,
  active_ind STRING,
  CONSTRAINT pk_version_forecast_mapping PRIMARY KEY (version_forecast_mapping_id))
USING delta
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd');


CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.company_code (
    company_id bigint NOT NULL PRIMARY KEY,
    company_cd string,
    company_nm string,
    currency_cd string,
    created_by_user_id string,
    updated_by_user_id string,
    physical_source_cd string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.copa_attribution_dim (
    copa_attribution_id bigint NOT NULL PRIMARY KEY,
    responsive_business_model_cd string,
    responsive_business_model_desc string,
    demand_stream_cd string,
    demand_stream_desc string,
    business_type_cd string,
    business_type_desc string,
    marketing_type_cd string,
    marketing_type_desc string,
    gender_age_cd string,
    gender_age_desc string,
    direct_business_model_cd string,
    direct_business_model_desc string,
    product_lifecycle_cd string,
    product_lifecycle_desc string,
    quality_cd string,
    quality_desc string,
    region_summary_product_group_cd string,
    region_summary_product_group_desc string,
    sales_order_reason_desc string,
    sales_order_type_cd string,
    sales_order_type_desc string,
    sales_order_type_group_desc string,
    sales_order_item_category_cd string,
    sales_order_item_category_desc string,
    distribution_method_cd string,
    distribution_method_desc string,
    sales_order_reason_cd string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.cost_center_dim_v (
    cost_center_nbr varchar(255) NOT NULL PRIMARY KEY,
    controlling_area_cd string,
    valid_to_dt date,
    valid_from_dt date,
    iso_language_cd string,
    cost_center_nm string,
    cost_center_desc string,
    cost_center_category_hierarchy_1_cd string,
    cost_center_category_hierarchy_2_cd string,
    company_cd string,
    source_system string,
    cost_center_type_cd string,
    cost_center_category_short_desc string,
    business_area_cd string,
    tax_jurisdiction_cd string,
    functional_area_cd string,
    currency_cd string,
    posting_allowed_ind string,
    planning_allowed_ind string,
    secondary_costs_posting_allowed_ind string,
    revenue_posting_allowed_ind string,
    commitment_update_allowed_ind string,
    secondary_costs_planning_allowed_ind string,
    revenue_planning_allowed_ind string,
    quantity_required_ind string,
    department_nm string,
    cost_center_report_printer_destination_cd string,
    company_legal_entity_id string,
    profit_center_nbr string,
    responsible_user_nm string,
    responsible_user_id string,
    responsible_user_title string,
    line_1_nm string,
    line_2_nm string,
    line_3_nm string,
    line_4_nm string,
    country_cd string,
    region_cd string,
    city_nm string,
    district_nm string,
    postal_cd string,
    street_address_txt string,
    po_box_postal_cd string,
    po_box_nbr string,
    correspondence_language_cd string,
    first_telephone_nbr string,
    second_telephone_nbr string,
    telebox_nbr string,
    fax_nbr string,
    teletex_nbr string,
    telex_nbr string,
    data_communication_line_nbr string,
    msg_header_tmst date,
    begin_effective_dt date,
    end_effective_dt date,
    cost_center_id bigint,
    _cost_center_cleansed_latest_load_timestamp date
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.dischannel_cost_center_hierarchy_dim_v (
    cost_center_hierarchy_hist_id bigint NOT NULL PRIMARY KEY,
    cost_center_nbr string,
    cost_center_hierarchy_nm string,
    cost_center_level_1_cd string,
    cost_center_level_1_nm string,
    cost_center_level_2_cd string,
    cost_center_level_2_nm string,
    cost_center_level_3_cd string,
    cost_center_level_3_nm string,
    cost_center_level_4_cd string,
    cost_center_level_4_nm string,
    cost_center_level_5_cd string,
    cost_center_level_5_nm string,
    cost_center_level_6_cd string,
    cost_center_level_6_nm string,
    cost_center_level_7_cd string,
    cost_center_level_7_nm string,
    cost_center_level_8_cd string,
    cost_center_level_8_nm string,
    cost_center_level_9_cd string,
    cost_center_level_9_nm string,
    cost_center_level_10_cd string,
    cost_center_level_10_nm string,
    cost_center_level_11_cd string,
    cost_center_level_11_nm string,
    cost_center_level_12_cd string,
    cost_center_level_12_nm string,
    cost_center_level_13_cd string,
    cost_center_level_13_nm string,
    cost_center_level_14_cd string,
    cost_center_level_14_nm string,
    cost_center_level_15_cd string,
    cost_center_level_15_nm string,
    cost_center_level_16_cd string,
    cost_center_level_16_nm string,
    cost_center_level_17_cd string,
    cost_center_level_17_nm string,
    cost_center_level_18_cd string,
    cost_center_level_18_nm string,
    cost_center_level_19_cd string,
    cost_center_level_19_nm string,
    cost_center_level_20_cd string,
    cost_center_level_20_nm string,
    cost_center_level_21_cd string,
    cost_center_level_21_nm string,
    cost_center_level_22_cd string,
    cost_center_level_22_nm string,
    cost_center_level_23_cd string,
    cost_center_level_23_nm string,
    cost_center_level_24_cd string,
    cost_center_level_24_nm string,
    cost_center_level_25_cd string,
    cost_center_level_25_nm string,
    cost_center_level_26_cd string,
    cost_center_level_26_nm string,
    cost_center_level_27_cd string,
    cost_center_level_27_nm string,
    cost_center_level_28_cd string,
    cost_center_level_28_nm string,
    cost_center_level_29_cd string,
    cost_center_level_29_nm string,
    cost_center_level_30_cd string,
    cost_center_level_30_nm string,
    controlling_area_cd string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.dischannel_profit_center_hierarchy (
    profit_center_hierarchy_id bigint NOT NULL PRIMARY KEY,
    distrchnl_profit_center_nbr string,
    controlling_area_cd string,
    profit_center_hierarchy_nm string,
    profit_center_level_1_cd string,
    profit_center_level_1_nm string,
    profit_center_level_2_cd string,
    profit_center_level_2_nm string,
    profit_center_level_3_cd string,
    profit_center_level_3_nm string,
    profit_center_level_4_cd string,
    profit_center_level_4_nm string,
    profit_center_level_5_cd string,
    profit_center_level_5_nm string,
    profit_center_level_6_cd string,
    profit_center_level_6_nm string,
    profit_center_level_7_cd string,
    profit_center_level_7_nm string,
    profit_center_level_8_cd string,
    profit_center_level_8_nm string,
    profit_center_level_9_cd string,
    profit_center_level_9_nm string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.division_text_dim_v (
    division_nbr varchar(50) NOT NULL PRIMARY KEY,
    division_nm string,
    division_id bigint,
    division_group string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.finance_customer_dim_v (
    finance_customer_id bigint NOT NULL PRIMARY KEY,
    customer_nbr string,
    channel_desc string,
    customer_nm string,
    customer_owner_group_nm string,
    marketplace_channel_nm string,
    geo_marketplace_unit_nm string,
    integrated_business_planning_level_1_desc string,
    integrated_business_planning_level_2_desc string,
    integrated_business_planning_level_3_desc string,
    integrated_business_planning_mpu_desc string,
    sub_territory_nm string,
    customer_business_type_nm string,
    customer_subtype_nm string,
    partner_channel string,
    partner_sub_channel string,
    partner_account_classification string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.finance_foreign_currency_exchange_rate (
    finance_foreign_currency_exchange_rate_id bigint NOT NULL PRIMARY KEY,
    from_currency_cd string,
    exchange_rate_cd string,
    exchange_rate_nm string,
    exchange_rate decimal(38, 18),
    from_currency_nm string,
    to_currency_cd string,
    to_currency_nm string,
    active_ind string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.finance_product_dim_v (
    product_id bigint NOT NULL PRIMARY KEY,
    primary_platform_desc string,
    style_nm string,
    franchise_nm string,
    gender_desc string,
    global_category_core_focus_desc string,
    product_cd string,
    team_nm string,
    league_desc string,
    athlete_full_nm string,
    product_company_nm string,
    age_desc string,
    consumer_construct_dimension_nm string,
    fields_of_play_nm string,
    merchandising_classification_desc string,
    consumer_construct_segment_nm string,
    brand_nm string,
    sub_category_desc string,
    blank_usage_ind string,
    silhouette_desc string,
    silhouette_type_desc string,
    style_nbr string,
    consumer_construct_global_consumer_offense_nm string,
    active_ind string,
    created_by_user_id string,
    updated_by_user_id string,
    physical_source_cd string,
    global_sport_focus_derived_desc string,
    global_sport_focus_desc string,
    global_sport_sub_focus_desc string,
    sub_brand_desc string,
    sub_brand_cd string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact (
  general_ledger_fact_id BIGINT,
  fiscal_year_period_nbr INT,
  profit_center_id BIGINT,
  division_id BIGINT,
  version_forecast_mapping_id BIGINT,
  functional_area_id BIGINT,
  accounting_document_type_id BIGINT,
  product_id BIGINT,
  customer_id BIGINT,
  company_id BIGINT,
  copa_attribution_id BIGINT,
  __cost_center_nbr_fk_id BIGINT,
  cost_center_nbr STRING COLLATE UTF8_BINARY,
  geo_wholesale_value_business_id BIGINT,
  geo_marketplace_channel_id BIGINT,
  __gl_account_nbr_fk_id BIGINT,
  gl_account_nbr STRING COLLATE UTF8_BINARY,
  zfsm_measure_id BIGINT,
  etm_foreign_currency_exchange_rate_id BIGINT,
  gaap_foreign_currency_exchange_rate_id BIGINT,
  company_currency_amt DECIMAL(28,5),
  transaction_currency_amt DECIMAL(28,5),
  performance_management_currency_amt DECIMAL(28,5),
  etm_ind INT,
  sales_qty DECIMAL(28,5),
  returns_qty DECIMAL(28,5),
  general_ledger_fact_ind STRING COLLATE UTF8_BINARY,
  cis_delta_ind STRING COLLATE UTF8_BINARY,
  general_ledger_ocogs_allocation_fact_ind STRING COLLATE UTF8_BINARY,
  anaplan_corporate_ind STRING COLLATE UTF8_BINARY,
  company_currency_cd STRING COLLATE UTF8_BINARY,
  transaction_currency_cd STRING COLLATE UTF8_BINARY,
  Functional_Area_cd STRING COLLATE UTF8_BINARY,
  accounting_document_type_cd STRING COLLATE UTF8_BINARY,
  profit_center_nbr STRING COLLATE UTF8_BINARY,
  company_cd STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd');

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.geo_marketplace_channel_dim (
    geo_marketplace_channel_id bigint NOT NULL PRIMARY KEY,
    geo_marketplace_channel_nm string,
    created_by_user_id string,
    updated_by_user_id string,
    physical_source_cd string,
    active_ind string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.geo_wholesale_value_business_dim (
    geo_wholesale_value_business_id bigint NOT NULL PRIMARY KEY,
    geo_wholesale_value_business_desc string,
    created_by_user_id string,
    updated_by_user_id string,
    physical_source_cd string
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.gl_account_dim (
    gl_account_nbr varchar(255) NOT NULL PRIMARY KEY,
    gl_account_short_desc string,
    gl_account_long_desc string,
    begin_effective_dt date,
    end_effective_dt date,
    active_ind string,
    gl_accnt_id bigint,
    cost_component_calc string
);


CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.gl_account_zfsm_measures_hierarchy_dim (
    zfsm_measure_id bigint NOT NULL PRIMARY KEY,
    created_by_user_id string,
    updated_by_user_id string,
    physical_source_cd string,
    active_ind string,
    zfsm_measure_cd string,
    zfsm_measure_desc string,
    gl_account_level_1_cd string,
    gl_account_level_1_nm string,
    gl_account_level_2_cd string,
    gl_account_level_2_nm string,
    gl_account_level_3_cd string,
    gl_account_level_3_nm string,
    gl_account_level_4_cd string,
    gl_account_level_4_nm string,
    gl_account_level_5_cd string,
    gl_account_level_5_nm string,
    gl_account_level_6_cd string,
    gl_account_level_6_nm string,
    gl_account_level_7_cd string,
    gl_account_level_7_nm string,
    gl_account_level_8_cd string,
    gl_account_level_8_nm string,
    gl_account_level_9_cd string,
    gl_account_level_9_nm string,
    gl_account_level_10_cd string,
    gl_account_level_10_nm string,
    gl_account_level_11_cd string,
    gl_account_level_11_nm string,
    gl_account_level_12_cd string,
    gl_account_level_12_nm string,
    gl_account_level_13_cd string,
    gl_account_level_13_nm string,
    record_created_tmst_utc date,
    record_update_tmst_utc date
);

CREATE OR REPLACE TABLE development.dev_pbi_perform_cf_poc_25b.profit_center (
    profit_center_id bigint NOT NULL PRIMARY KEY,
    profit_center_nbr string,
    profit_center_nm string,
    segment_id string,
    geography_nm string,
    profit_center_channel_nm string,
    territory_nm string,
    sub_territory_nm string,
    begin_effective_dt date,
    end_effective_dt date,
    active_ind string,
    geography_sort int,
    operating_segment_nm string
);


ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_calendar_fiscal_period_v_fiscal_year_period_nbr_to_general_ledger_fact_fiscal_year_period_nbr FOREIGN KEY (fiscal_year_period_nbr) REFERENCES development.dev_pbi_perform_cf_poc_25b.calendar_fiscal_period_v(fiscal_year_period_nbr);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_profit_center_profit_center_id_to_general_ledger_fact_profit_center_id FOREIGN KEY (profit_center_id) REFERENCES development.dev_pbi_perform_cf_poc_25b.profit_center(profit_center_id);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_version_forecast_mapping_version_forecast_mapping_id_to_general_ledger_fact_version_forecast_mapping_id FOREIGN KEY (version_forecast_mapping_id) REFERENCES development.dev_pbi_perform_cf_poc_25b.version_forecast_mapping(version_forecast_mapping_id);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_finance_product_dim_v_product_id_to_general_ledger_fact_product_id FOREIGN KEY (product_id) REFERENCES development.dev_pbi_perform_cf_poc_25b.finance_product_dim_v(product_id);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_finance_customer_dim_v_finance_customer_id_to_general_ledger_fact_customer_id FOREIGN KEY (customer_id) REFERENCES development.dev_pbi_perform_cf_poc_25b.finance_customer_dim_v(finance_customer_id);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_company_code_company_id_to_general_ledger_fact_company_id FOREIGN KEY (company_id) REFERENCES development.dev_pbi_perform_cf_poc_25b.company_code(company_id);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_copa_attribution_dim_copa_attribution_id_to_general_ledger_fact_copa_attribution_id FOREIGN KEY (copa_attribution_id) REFERENCES development.dev_pbi_perform_cf_poc_25b.copa_attribution_dim(copa_attribution_id);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_cost_center_dim_v_cost_center_nbr_to_general_ledger_fact_cost_center_nbr FOREIGN KEY (cost_center_nbr) REFERENCES development.dev_pbi_perform_cf_poc_25b.cost_center_dim_v(cost_center_nbr);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_geo_wholesale_value_business_dim_geo_wholesale_value_business_id_to_general_ledger_fact_geo_wholesale_value_business_id FOREIGN KEY (geo_wholesale_value_business_id) REFERENCES development.dev_pbi_perform_cf_poc_25b.geo_wholesale_value_business_dim(geo_wholesale_value_business_id);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_geo_marketplace_channel_dim_geo_marketplace_channel_id_to_general_ledger_fact_geo_marketplace_channel_id FOREIGN KEY (geo_marketplace_channel_id) REFERENCES development.dev_pbi_perform_cf_poc_25b.geo_marketplace_channel_dim(geo_marketplace_channel_id);

ALTER TABLE development.dev_pbi_perform_cf_poc_25b.general_ledger_fact
ADD CONSTRAINT fk_gl_account_dim_gl_account_nbr_to_general_ledger_fact_gl_account_nbr FOREIGN KEY (gl_account_nbr) REFERENCES development.dev_pbi_perform_cf_poc_25b.gl_account_dim(gl_account_nbr);
