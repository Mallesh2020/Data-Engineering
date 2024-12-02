SELECT * FROM alpha_num;

SELECT *, CASE 
              WHEN UPPER(val) = LOWER(val) THEN 'Number'
              ELSE 'Alphabet'
		  END AS category
FROM alpha_num;