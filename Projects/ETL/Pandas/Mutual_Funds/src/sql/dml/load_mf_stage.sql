INSERT IGNORE INTO mutual_fund_analysis.mf_stage
(
    mutual_fund_category,
    mutual_fund_name,
    scheme_code,
    scheme_name,
    isin_div_payout_growth,
    isin_div_reinvestment,
    net_asset_value,
    repurchase_price,
    sale_price,
    record_date
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)