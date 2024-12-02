CREATE TABLE Employee (
    Emp_Id INT PRIMARY KEY,
    Emp_name VARCHAR(50),
    Salary INT,
    Manager_Id INT
);


INSERT INTO Employee (Emp_Id, Emp_name, Salary, Manager_Id) VALUES
(10, 'Anil', 50000, 18),
(11, 'Vikas', 75000, 16),
(12, 'Nisha', 40000, 18),
(13, 'Nidhi', 60000, 17),
(14, 'Priya', 80000, 18),
(15, 'Mohit', 45000, 18),
(16, 'Rajesh', 90000, NULL), -- Assuming "–" means NULL for the manager.
(17, 'Raman', 55000, 16),
(18, 'Santosh', 65000, 17);