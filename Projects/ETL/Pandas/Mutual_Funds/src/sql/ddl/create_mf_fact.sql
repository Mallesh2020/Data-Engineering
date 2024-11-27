CREATE TABLE IF NOT EXISTS mutual_fund_analysis.mutualfunds_nav
(
    scheme_id INT,
    category_id INT,
    isin_div_payout_growth VARCHAR(20),
    isin_div_reinvestment VARCHAR(20),
    net_asset_value DECIMAL(10, 4),
    repurchase_price DECIMAL(10, 4),
    sale_price DECIMAL(10, 4),
    record_date DATE,
    PRIMARY KEY (scheme_id, record_date),
    FOREIGN KEY (category_id) REFERENCES mutual_fund_analysis.categories(category_id),
    FOREIGN KEY (scheme_id) REFERENCES mutual_fund_analysis.schemes(scheme_id)
)