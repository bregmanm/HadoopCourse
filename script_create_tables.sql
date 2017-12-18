DROP DATABASE IF EXISTS sqoop1;
CREATE DATABASE sqoop1;
USE sqoop1
DROP TABLE IF EXISTS MOST_FREQ_CAT;
CREATE TABLE MOST_FREQ_CAT (
    category varchar(255),
    cat_occur bigint
);
DROP TABLE IF EXISTS MOST_FREQ_PROD;
CREATE TABLE MOST_FREQ_PROD (
    category varchar(255),
    name varchar(255),
    name_occur bigint
);
DROP TABLE IF EXISTS COUNTRIES_MOST_PURCHASED;
CREATE TABLE COUNTRIES_MOST_PURCHASED (
    country_name varchar(255),
    spend  numeric(10,2)
)
