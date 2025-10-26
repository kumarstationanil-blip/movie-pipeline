DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS genres;

CREATE TABLE movies (
    movie_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    release_year INTEGER,
    genre_id INTEGER,
    FOREIGN KEY (genre_id) REFERENCES genres (genre_id)
);

CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY,
    genre_name TEXT NOT NULL
);

CREATE TABLE ratings (
    rating_id INTEGER PRIMARY KEY,
    movie_id INTEGER,
    user_id INTEGER,
    rating REAL,
    timestamp TEXT,
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
);
