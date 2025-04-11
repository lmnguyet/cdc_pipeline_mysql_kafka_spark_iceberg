#!/bin/bash
echo "Importing CSV into MySQL..."

mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "
CREATE DATABASE IF NOT EXISTS metastore;
GRANT ALL PRIVILEGES ON metastore.* TO 'lminhnguyet'@'%';
FLUSH PRIVILEGES;
USE $MYSQL_DATABASE;
CREATE TABLE films (
    number INT AUTO_INCREMENT PRIMARY KEY,
    film VARCHAR(100) NOT NULL,
    release_date DATE,
    run_time INT,
    film_rating VARCHAR(10),
    plot TEXT
);
CREATE TABLE film_ratings (
    film VARCHAR(100) NOT NULL,
    rotten_tomatoes_score INT,
    rotten_tomatoes_counts INT,
    metacritic_score INT,
    metacritic_counts INT,
    cinema_score VARCHAR(5),
    imdb_score DECIMAL(3,1),
    imdb_counts INT
);
CREATE TABLE genres (
    film VARCHAR(100) NOT NULL,
    category VARCHAR(100),
    value VARCHAR(100)
);
CREATE TABLE box_office (
    film VARCHAR(100) NOT NULL,
    budget INT,
    box_office_us_canada INT,
    box_office_other INT,
    box_office_worldwide INT
);
LOAD DATA INFILE '/var/lib/mysql-files/pixar_films.csv'
INTO TABLE films
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA INFILE '/var/lib/mysql-files/public_response.csv'
INTO TABLE film_ratings
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA INFILE '/var/lib/mysql-files/genres.csv'
INTO TABLE genres
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
LOAD DATA INFILE '/var/lib/mysql-files/box_office.csv'
INTO TABLE box_office
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(film, @budget, @box_office_us_canada, @box_office_other, @box_office_worldwide)
SET 
    budget = IF(@budget = 'NA', NULL, @budget),
    box_office_us_canada = IF(@box_office_us_canada = 'NA', NULL, @box_office_us_canada),
    box_office_other = IF(@box_office_other = 'NA', NULL, @box_office_other),
    box_office_worldwide = IF(@box_office_worldwide = 'NA', NULL, @box_office_worldwide)
;
"

echo "CSV import completed."

