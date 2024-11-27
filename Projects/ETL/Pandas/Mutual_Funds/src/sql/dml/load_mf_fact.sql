INSERT IGNORE INTO mutual_fund_analysis.mutualfunds_nav
SELECT sc.scheme_id, 
       cat.category_id,
       stg.isin_div_payout_growth,
       stg.isin_div_reinvestment,
       stg.net_asset_value,
       stg.repurchase_price,
       stg.sale_price,
       stg.record_date
FROM mutual_fund_analysis.mf_stage stg 
INNER JOIN mutual_fund_analysis.categories cat 
      ON stg.mutual_fund_category = cat.mutual_fund_category
INNER JOIN mutual_fund_analysis.schemes sc
      ON stg.scheme_name = sc.scheme_name