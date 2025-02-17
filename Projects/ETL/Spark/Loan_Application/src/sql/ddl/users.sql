CREATE TABLE IF NOT EXISTS loan_app.users (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(15),
    date_of_birth DATE,
    address VARCHAR(255),
    account_status ENUM('Active', 'Deleted'),
    sign_up_date TIMESTAMP,
    account_closed_date DATE NULL,
    updated_at TIMESTAMP
);
