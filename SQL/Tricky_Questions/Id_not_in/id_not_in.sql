-- Create table for runners
CREATE TABLE runners (
    id INT PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

-- Create table for races
CREATE TABLE races (
    id INT PRIMARY KEY,
    event VARCHAR(50) NOT NULL,
    winner_id INT,
    FOREIGN KEY (winner_id) REFERENCES runners(id)
);

-- Insert data into runners table
INSERT INTO runners (id, name) VALUES
(1, 'John Doe'),
(2, 'Jane Doe'),
(3, 'Alice Jones'),
(4, 'Bobby Louis'),
(5, 'Lisa Romero');

-- Insert data into races table
INSERT INTO races (id, event, winner_id) VALUES
(1, '100 meter dash', 2),
(2, '500 meter dash', 3),
(3, 'cross-country', 2),
(4, 'triathalon', NULL);

