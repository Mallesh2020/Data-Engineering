CREATE TABLE IF NOT EXISTS loan_app.loan_offers (
    loan_id INT PRIMARY KEY,
    loan_amount INT NOT NULL,
    tenure INT NOT NULL,
    interest_rate DOUBLE NOT NULL,
    total_amount DECIMAL(15,2) NOT NULL,
    offer_status ENUM('Expired', 'Active') NOT NULL,
    updated_at TIMESTAMP
);