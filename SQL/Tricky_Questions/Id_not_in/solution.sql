SELECT * FROM runners;
SELECT * FROM races;

-- Query
SELECT * FROM runners 
WHERE id NOT IN (SELECT winner_id FROM races); -- No Records

-- Correct Query
SELECT * FROM runners 
WHERE id NOT IN (SELECT winner_id 
                 FROM races 
                 WHERE winner_id IS NOT NULL);