CREATE TABLE IF NOT EXISTS loan_app.defaulters (
    defaulter_id INT NOT NULL,
    loan_id INT NOT NULL,
    loan_amount DECIMAL(15,2) NOT NULL,  -- Total loan amount
    remaining_balance DECIMAL(15,2) NOT NULL,  -- Loan amount - Debit transactions
    days_past_due INT NOT NULL,  -- Current date - Due date
    updated_at TIMESTAMP,
    PRIMARY KEY (defaulter_id, loan_id),
    FOREIGN KEY (defaulter_id) REFERENCES users(user_id),
    FOREIGN KEY (loan_id) REFERENCES loan_offers(loan_id)
);