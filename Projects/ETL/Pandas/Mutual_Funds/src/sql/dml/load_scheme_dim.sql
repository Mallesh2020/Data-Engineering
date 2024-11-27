INSERT IGNORE INTO mutual_fund_analysis.schemes (category_id, mutual_fund_name, scheme_code, scheme_name)
SELECT cat.category_id, 
       st.mutual_fund_name,
       st.scheme_code,
       st.scheme_name
FROM mutual_fund_analysis.mf_stage st 
INNER JOIN mutual_fund_analysis.categories cat
ON st.mutual_fund_category = cat.mutual_fund_category