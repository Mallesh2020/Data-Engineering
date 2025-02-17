CREATE TABLE IF NOT EXISTS loan_app.loan_balances (
    loan_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    loan_amount DECIMAL(15,2) NOT NULL,  -- Total loan amount
    due_date DATE NOT NULL,  -- Approved Date + Tenure in Months
    total_borrow DECIMAL(15,2) DEFAULT 0,  -- Total disbursed amount (Credit transactions)
    total_repayment DECIMAL(15,2) DEFAULT 0,   -- Total repaid amount (Debit transactions)
    remaining_balance DECIMAL(15,2) NOT NULL,  -- Loan amount - Debit transactions
    days_past_due INT NOT NULL,  -- Current date - Due date
    updated_at TIMESTAMP,
    FOREIGN KEY (loan_id) REFERENCES user_loans(loan_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
