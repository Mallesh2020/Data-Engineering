CREATE TABLE IF NOT EXISTS mutual_fund_analysis.schemes
(
    scheme_id INT AUTO_INCREMENT PRIMARY KEY,
    category_id INT,
    mutual_fund_name VARCHAR(50),
    scheme_code INT UNIQUE,
    scheme_name VARCHAR(50),
    FOREIGN KEY (category_id) REFERENCES mutual_fund_analysis.categories(category_id)
)