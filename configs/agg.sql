SELECT
cstc.cost_center_nm AS a_distribution_channel_cost_center,
div_tbl.division_group AS a_division_group,
prd.global_sport_focus_desc AS a_global_sport_focus_desc,
cstc.cost_center_nm AS a_segment_cost_center,
glf.accounting_document_type_cd AS accounting_document_type_code,
glf.anaplan_corporate_ind AS anaplan_corporate_indicator,
glf.cis_delta_ind AS cis_delta_indicator,
cc.company_cd AS company_code,
glf.company_currency_cd AS company_currency_code,
dcpch.distrchnl_profit_center_nbr AS distribution_channel_profit_center,
dcpch.distrchnl_profit_center_nbr AS distribution_channel_profit_center_dup,
fcer.finance_foreign_currency_exchange_rate_id AS finance_foreign_currency_exchange_rate_id,
t.fiscal_year_nbr AS fiscal_year,
t.fiscal_year_period_nbr AS fiscal_year_period,
t.fiscal_year_quarter_nbr AS fiscal_year_quarter,
glf.functional_area_cd AS functional_area_code,
glf.general_ledger_fact_ind AS general_ledger_fact_indicator,
glf.general_ledger_ocogs_allocation_fact_ind AS general_ledger_ocogs_allocation_fact_indicator,
spc.geography_nm AS geography,
zfsm.gl_account_level_10_nm AS gl_account_level_10_attribute,
zfsm.gl_account_level_11_cd AS gl_account_level_11,
zfsm.gl_account_level_12_nm AS gl_account_level_12_attribute,
zfsm.gl_account_level_8_nm AS gl_account_level_8_attribute,
zfsm.gl_account_level_9_nm AS gl_account_level_9_attribute,
gla.gl_account_nbr AS gl_account_number,
zfsm.gl_account_level_10_nm AS gl_acct_level_10_attribute,
zfsm.gl_account_level_11_nm AS gl_acct_level_11_attribute,
zfsm.gl_account_level_12_nm AS gl_acct_level_12_attribute,
zfsm.gl_account_level_8_nm AS gl_acct_level_8_attribute,
zfsm.gl_account_level_9_nm AS gl_acct_level_9_attribute,
SUM(glf.performance_management_currency_amt) AS iris_performance_management_currency_amount,
SUM(glf.returns_qty) AS iris_returns_units_calc,
SUM(glf.sales_qty) AS iris_sales_units_calc,
copa.copa_attribution_id AS l_copa_attribution_id,
cstc.cost_center_nbr AS l_cost_center_nbr,
cust.finance_customer_id AS l_customer_id,
dccch.cost_center_nbr AS l_distribution_channel_cost_center,
div_tbl.division_id AS l_division_id,
t.fiscal_year_nbr AS l_fiscal_year,
t.fiscal_year_period_nbr AS l_fiscal_year_period,
t.fiscal_year_quarter_nbr AS l_fiscal_year_quarter,
gmc.geo_marketplace_channel_id AS l_geo_marketplace_channel_id,
zfsm.gl_account_level_10_cd AS l_gl_account_level_10,
zfsm.gl_account_level_11_cd AS l_gl_account_level_11,
zfsm.gl_account_level_12_cd AS l_gl_account_level_12,
zfsm.gl_account_level_8_cd AS l_gl_account_level_8,
zfsm.gl_account_level_9_cd AS l_gl_account_level_9,
scch.cost_center_nbr AS l_segment_cost_center,
spc.profit_center_nbr AS l_segment_profit_center,
zfsm.zfsm_measure_id AS l_zfsm_measures_id,
pc.profit_center_channel_nm AS profit_center_channel,
pc.profit_center_nbr AS profit_center_number,
spch.profit_center_nbr AS segment_profit_center,
spch.profit_center_nbr AS segment_profit_center_dup,
spc.sub_territory_nm AS sub_territory,
spc.territory_nm AS territory,
glf.transaction_currency_cd AS transaction_currency_code,
v.version_forecast_mapping_id AS version_forecast_mapping_id,
zfsm.zfsm_measure_cd AS zfsm_measure_cd

FROM development.dev_pbi_perform_cf_poc.general_ledger_fact_2B glf

 JOIN development.dev_pbi_perform_cf_poc.profit_center pc
       ON glf.profit_center_nbr = pc.profit_center_nbr

 JOIN development.dev_pbi_perform_cf_poc.dischannel_profit_center_hierarchy dcpch
       ON glf.profit_center_nbr = dcpch.distrchnl_profit_center_nbr

 JOIN development.dev_pbi_perform_cf_poc.segment_profit_center_hierarchy spch
       ON glf.profit_center_nbr = spch.profit_center_nbr

 JOIN development.dev_pbi_perform_cf_poc.calendar_fiscal_period_v t
       ON glf.fiscal_year_period_nbr = t.fiscal_year_period_nbr

 JOIN development.dev_pbi_perform_cf_poc.gl_account_zfsm_measures_hierarchy_dim zfsm
       ON glf.zfsm_measure_id = zfsm.zfsm_measure_id

 JOIN development.dev_pbi_perform_cf_poc.finance_foreign_currency_exchange_rate fcer
       ON glf.etm_foreign_currency_exchange_rate_id = fcer.finance_foreign_currency_exchange_rate_id

 JOIN development.dev_pbi_perform_cf_poc.gl_account_dim gla
       ON glf.gl_account_nbr = gla.gl_account_nbr

 JOIN development.dev_pbi_perform_cf_poc.version_forecast_mapping v
       ON glf.version_forecast_mapping_id = v.version_forecast_mapping_id

 JOIN development.dev_pbi_perform_cf_poc.profit_center spc
       ON glf.profit_center_nbr = spc.profit_center_nbr

 JOIN development.dev_pbi_perform_cf_poc.finance_product_dim_v prd
       ON glf.product_id = prd.product_id

 JOIN development.dev_pbi_perform_cf_poc.finance_customer_dim_v cust
       ON glf.customer_id = cust.finance_customer_id

 JOIN development.dev_pbi_perform_cf_poc.company_code cc
       ON glf.company_cd = cc.company_cd

 JOIN development.dev_pbi_perform_cf_poc.copa_attribution_dim copa
       ON glf.copa_attribution_id = copa.copa_attribution_id

 JOIN development.dev_pbi_perform_cf_poc.division_text_dim_v div_tbl
       ON glf.division_id = div_tbl.division_id

 JOIN development.dev_pbi_perform_cf_poc.cost_center_dim_v cstc
       ON glf.cost_center_nbr = cstc.cost_center_nbr

 JOIN development.dev_pbi_perform_cf_poc.dischannel_cost_center_hierarchy_dim_v dccch
       ON glf.cost_center_nbr = dccch.cost_center_nbr

 JOIN development.dev_pbi_perform_cf_poc.segment_cost_center_hierarchy_dim_v scch
       ON glf.cost_center_nbr = scch.cost_center_nbr

 JOIN development.dev_pbi_perform_cf_poc.geo_marketplace_channel_dim gmc
       ON glf.geo_marketplace_channel_id = gmc.geo_marketplace_channel_id

GROUP BY
    fcer.finance_foreign_currency_exchange_rate_id,
    pc.profit_center_nbr,
    pc.profit_center_channel_nm,
    t.fiscal_year_period_nbr,
    t.fiscal_year_nbr,
    t.fiscal_year_quarter_nbr,
    dcpch.distrchnl_profit_center_nbr,
    spch.profit_center_nbr,
    zfsm.zfsm_measure_id,
    zfsm.zfsm_measure_cd,
    zfsm.gl_account_level_8_cd,
    zfsm.gl_account_level_9_cd,
    zfsm.gl_account_level_10_cd,
    zfsm.gl_account_level_11_cd,
    zfsm.gl_account_level_12_cd,
    zfsm.gl_account_level_8_nm,
    zfsm.gl_account_level_9_nm,
    zfsm.gl_account_level_10_nm,
    zfsm.gl_account_level_12_nm,
    zfsm.gl_account_level_11_nm,
    spc.profit_center_nbr,
    spc.geography_nm,
    spc.territory_nm,
    spc.sub_territory_nm,
    cc.company_cd,
    glf.general_ledger_fact_ind,
    glf.cis_delta_ind,
    glf.transaction_currency_cd,
    glf.company_currency_cd,
    glf.general_ledger_ocogs_allocation_fact_ind,
    glf.anaplan_corporate_ind,
    glf.functional_area_cd,
    glf.accounting_document_type_cd,
    v.version_forecast_mapping_id,
    gla.gl_account_nbr,
    cust.finance_customer_id,
    copa.copa_attribution_id,
    div_tbl.division_id,
    div_tbl.division_group,
    prd.global_sport_focus_desc,
    cstc.cost_center_nbr,
    dccch.cost_center_nbr,
    cstc.cost_center_nm,
    scch.cost_center_nbr,
    gmc.geo_marketplace_channel_id;