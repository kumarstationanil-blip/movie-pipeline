-- ============================================
-- Task 5: Analytical Queries
-- ============================================

-- 1️⃣ Which movie has the highest average rating?
SELECT 
    title,
    ROUND(AVG(rating), 2) AS avg_rating
FROM etl_movie_data
GROUP BY title
ORDER BY avg_rating DESC
LIMIT 1;

-- 2️⃣ What are the top 5 movie genres that have the highest average rating?
SELECT 
    genre,
    ROUND(AVG(rating), 2) AS avg_rating
FROM etl_movie_data
WHERE genre IS NOT NULL AND genre != 'Unknown'
GROUP BY genre
ORDER BY avg_rating DESC
LIMIT 5;

-- 3️⃣ Who is the director with the most movies in this dataset?
SELECT 
    director,
    COUNT(DISTINCT title) AS total_movies
FROM etl_movie_data
WHERE director IS NOT NULL AND director != 'Unknown'
GROUP BY director
ORDER BY total_movies DESC
LIMIT 1;

-- 4️⃣ What is the average rating of movies released each year?
SELECT 
    year,
    ROUND(AVG(rating), 2) AS avg_rating
FROM etl_movie_data
WHERE year IS NOT NULL AND year != 'Unknown'
GROUP BY year
ORDER BY year ASC;
