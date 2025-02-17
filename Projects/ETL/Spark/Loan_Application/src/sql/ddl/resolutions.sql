CREATE TABLE IF NOT EXISTS loan_app.resolutions (
    executive_id INT,
    defaulter_id INT,
    executive_name VARCHAR(100),
    contact_number VARCHAR(15),
    is_resolved ENUM('Yes', 'No'),
    assigned_at TIMESTAMP,
    updated_at TIMESTAMP,
    FOREIGN KEY (defaulter_id) REFERENCES users(user_id)
);