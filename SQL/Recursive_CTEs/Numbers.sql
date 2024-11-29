WITH RECURSIVE numbers AS 
(
 SELECT 1 as nums
 UNION 
 SELECT nums+1 FROM numbers
 WHERE nums < 10
)

SELECT * FROM numbers