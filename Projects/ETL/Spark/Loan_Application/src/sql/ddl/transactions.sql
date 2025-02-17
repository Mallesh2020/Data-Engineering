CREATE TABLE IF NOT EXISTS loan_app.transactions (
    transaction_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    loan_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    transaction_type ENUM('Borrow', 'Repay') NOT NULL, 
    transaction_date TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (loan_id) REFERENCES user_loans(loan_id)
);