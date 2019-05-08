CREATE TABLE netflix_data (
	user_id INT,
	rating CHAR(1),
	date DATE(),
	movie_id INT, 
	PRIMARY KEY (user_id, rating, year, movie_id) 
);