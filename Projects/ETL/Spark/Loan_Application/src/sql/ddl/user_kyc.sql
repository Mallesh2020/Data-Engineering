CREATE TABLE IF NOT EXISTS loan_app.user_kyc (
    kyc_id INT PRIMARY KEY,
    user_id INT,
    document_type VARCHAR(50),
    document_number VARCHAR(50),
    kyc_status ENUM('Fail', 'Success', 'Pending'),
    submitted_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);