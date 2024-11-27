CREATE TABLE IF NOT EXISTS mutual_fund_analysis.categories
(
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    mutual_fund_category VARCHAR(50) UNIQUE
)