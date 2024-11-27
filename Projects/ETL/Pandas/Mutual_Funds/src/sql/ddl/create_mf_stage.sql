CREATE TABLE IF NOT EXISTS mutual_fund_analysis.mf_stage
(
    mutual_fund_category VARCHAR(50),
    mutual_fund_name VARCHAR(50),
    scheme_code INT,
    scheme_name VARCHAR(50),
    isin_div_payout_growth VARCHAR(20),
    isin_div_reinvestment VARCHAR(20),
    net_asset_value DECIMAL(10, 4),
    repurchase_price DECIMAL(10, 4),
    sale_price DECIMAL(10, 4),
    record_date DATE,
    PRIMARY KEY (scheme_code, record_date)
);