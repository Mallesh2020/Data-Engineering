SELECT * FROM Employee;

SELECT m.emp_id, m.emp_name, AVG(e.salary) AS Avg_Sal_Under_Manager
FROM Employee e INNER JOIN Employee m 
ON e.manager_id = m.emp_id
GROUP BY m.emp_id 
ORDER BY m.emp_id