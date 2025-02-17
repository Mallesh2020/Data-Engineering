CREATE TABLE IF NOT EXISTS loan_app.user_loans (
    loan_id INT NOT NULL,
    user_id INT NOT NULL,
    status ENUM('Applied', 'Approved', 'Rejected', 'Active', 'Closed', 'Defaulted') NOT NULL,
    applied_date DATETIME,
    approved_date DATETIME,
    updated_at TIMESTAMP,
    PRIMARY KEY (user_id, loan_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (loan_id) REFERENCES loan_offers(loan_id)
);