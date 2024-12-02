SELECT * FROM Customer;

SELECT GROUP_CONCAT(CustomerName) AS all_customers
FROM Customer;

SELECT GROUP_CONCAT(CustomerName SEPARATOR ' | ') AS all_customers
FROM Customer