-- Create the database
CREATE DATABASE master;

-- Use the database
USE master;


-- Create table for data
CREATE TABLE netflix (
    show_id VARCHAR(10) PRIMARY KEY,
    type VARCHAR(10),
    title VARCHAR(200),
    director VARCHAR(250),
    cast VARCHAR(1000),
    country VARCHAR(150),
    date_added VARCHAR(20),
    release_year INT,
    rating VARCHAR(10),
    duration VARCHAR(10),
    listed_in VARCHAR(100),
    description VARCHAR(500)
);

-- Select all records from Netflix
SELECT * FROM netflix;

-- Remove duplicates
SELECT show_id, COUNT(*)
FROM netflix
GROUP BY show_id
HAVING COUNT(*) > 1;

SELECT title, COUNT(*)
FROM netflix
GROUP BY title
HAVING COUNT(*) > 1;

SELECT * FROM netflix
WHERE CONCAT(title, type) IN (
    SELECT CONCAT(title, type)
    FROM netflix
    GROUP BY CONCAT(title, type)
    HAVING COUNT(*) > 1
)
ORDER BY title;

SELECT title, type
FROM netflix
GROUP BY title, type
HAVING COUNT(*) > 1;

WITH cte AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix
)
SELECT *
FROM cte
WHERE rn = 1;


-- New tables for "listed_in", "director", "country", and "cast"
CREATE TABLE netflix_directors AS
SELECT show_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(director, ',', n.n), ',', -1)) AS director
FROM netflix CROSS JOIN (
    SELECT a.N + b.N * 10 + 1 AS n
    FROM (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
    CROSS JOIN (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
) n
WHERE n.n <= 1 + LENGTH(director) - LENGTH(REPLACE(director, ',', ''));

CREATE TABLE netflix_genre AS
SELECT show_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(listed_in, ',', n.n), ',', -1)) AS genre
FROM netflix CROSS JOIN (
    SELECT a.N + b.N * 10 + 1 AS n
    FROM (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
    CROSS JOIN (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
) n
WHERE n.n <= 1 + LENGTH(listed_in) - LENGTH(REPLACE(listed_in, ',', ''));

CREATE TABLE netflix_countries AS
SELECT show_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(country, ',', n.n), ',', -1)) AS country
FROM netflix CROSS JOIN (
    SELECT a.N + b.N * 10 + 1 AS n
    FROM (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
    CROSS JOIN (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
) n
WHERE n.n <= 1 + LENGTH(country) - LENGTH(REPLACE(country, ',', ''));

CREATE TABLE netflix_cast AS
SELECT show_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(cast, ',', n.n), ',', -1)) AS cast
FROM netflix CROSS JOIN (
    SELECT a.N + b.N * 10 + 1 AS n
    FROM (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
    CROSS JOIN (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
) n
WHERE n.n <= 1 + LENGTH(cast) - LENGTH(REPLACE(cast, ',', ''));

-- Populate missing values in "country"
SELECT *
FROM netflix
WHERE country IS NULL;

INSERT INTO netflix_countries
SELECT show_id, m.country
FROM netflix AS n
INNER JOIN (
    SELECT director, country
    FROM netflix_countries AS nc
    INNER JOIN netflix_directors AS nd ON nc.show_id = nd.show_id
    GROUP BY director, country
) AS m ON n.director = m.director
WHERE n.country IS NULL;


-- Netflix Data Analysis

-- 1. For each director, count the number of movies and TV shows created by them in separate columns 
--    for directors who have created both TV shows and movies.

SELECT nd.director,
       COUNT(DISTINCT CASE WHEN nf.type = 'Movie' THEN nf.show_id END) AS no_of_movies,
       COUNT(DISTINCT CASE WHEN nf.type = 'TV Show' THEN nf.show_id END) AS no_of_tvshow
FROM netflix_directors AS nd
INNER JOIN netflix_f AS nf ON nd.show_id = nf.show_id
GROUP BY nd.director
HAVING COUNT(DISTINCT nf.type) > 1;

-- 2. Which country has the highest number of comedy movies?

SELECT nc.country, 
       COUNT(DISTINCT ng.show_id) AS no_of_movies
FROM netflix_genre ng
INNER JOIN netflix_countries nc ON ng.show_id = nc.show_id
INNER JOIN netflix_f n ON ng.show_id = n.show_id
WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
GROUP BY nc.country
ORDER BY no_of_movies DESC
LIMIT 1;

-- 3. For each year (as per the date added to Netflix), which director has the maximum number of movies released?

WITH cte AS (
    SELECT nd.director,
           YEAR(n.date_added) AS date_year,
           COUNT(n.show_id) AS no_of_movies
    FROM netflix n
    INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
    WHERE n.type = 'Movie'
    GROUP BY nd.director, YEAR(n.date_added)
),
cte2 AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY date_year ORDER BY no_of_movies DESC, director) AS rn
    FROM cte
)
SELECT *
FROM cte2
WHERE rn = 1;

-- 4. What is the average duration of movies in each genre?

SELECT ng.genre, 
       AVG(CAST(REPLACE(duration, ' min', '') AS UNSIGNED)) AS avg_duration
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE n.type = 'Movie' AND duration LIKE '% min'
GROUP BY ng.genre
ORDER BY avg_duration DESC;


-- 5. Find the list of directors who have created both horror and comedy movies.
--    Display director names along with the number of comedy and horror movies directed by them.

SELECT nd.director,
       COUNT(DISTINCT CASE WHEN ng.genre = 'Comedies' THEN n.show_id END) AS no_of_comedy,
       COUNT(DISTINCT CASE WHEN ng.genre = 'Horror Movies' THEN n.show_id END) AS no_of_horror
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
WHERE n.type = 'Movie' AND ng.genre IN ('Comedies', 'Horror Movies')
GROUP BY nd.director
HAVING COUNT(DISTINCT ng.genre) = 2;

