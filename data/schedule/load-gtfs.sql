/*
Updated script by James Williams <https://jamesrwilliams.ca>
Original script contributed by Michael Perkins
https://github.com/sbma44/py-gtfs-mysql/blob/master/sql_better/load.sql
*/

CREATE DATABASE IF NOT EXISTS DUK_JR;

USE DUK_JR

DROP TABLE IF EXISTS agency;

CREATE TABLE `agency` (
	agency_id VARCHAR(255),
	agency_name VARCHAR(255),
	agency_url VARCHAR(255),
	agency_timezone VARCHAR(255),
	agency_lang VARCHAR(255),
	agency_phone VARCHAR(255),
	agency_fare_url VARCHAR(255)
);

DROP TABLE IF EXISTS calendar_dates;

CREATE TABLE `calendar_dates` (
	service_id VARCHAR(255),
	`date` VARCHAR(255),
	exception_type TINYINT(1)
);

DROP TABLE IF EXISTS calendar;

CREATE TABLE `calendar` (
    service_id VARCHAR(255),
    monday TINYINT(1),
    tuesday TINYINT(1),
    wednesday TINYINT(1),
    thursday TINYINT(1),
    friday TINYINT(1),
    saturday TINYINT(1),
    sunday TINYINT(1),
    start_date VARCHAR(255),
    end_date VARCHAR(255)
);

DROP TABLE IF EXISTS routes;

CREATE TABLE `routes` (
    route_id VARCHAR(255) PRIMARY KEY,
    agency_id VARCHAR(255),
    route_short_name VARCHAR(255),
    route_long_name VARCHAR(255),
    route_type VARCHAR(255),
    route_color VARCHAR(255),
    route_text_color VARCHAR(255)
);

DROP TABLE IF EXISTS stop_times;

CREATE TABLE `stop_times` (
    trip_id VARCHAR(255),
    arrival_time VARCHAR(255),
    departure_time VARCHAR(255),
    stop_id VARCHAR(255),
    stop_sequence VARCHAR(255),
    pickup_type VARCHAR(255),
    drop_off_type VARCHAR(255),
    stop_headsign VARCHAR(255)
);

DROP TABLE IF EXISTS stops;

CREATE TABLE `stops` (
    stop_id VARCHAR(255),
    stop_code VARCHAR(255),
    stop_name VARCHAR(255),
    stop_desc VARCHAR(255),
    stop_lat VARCHAR(255),
    stop_lon VARCHAR(255),
    zone_id VARCHAR(255),
    stop_url VARCHAR(255),
    location_type VARCHAR(255),
    parent_station VARCHAR(255),
    stop_timezone VARCHAR(255),
    wheelchair_boarding VARCHAR(255),
    platform_code VARCHAR(255)
);

DROP TABLE IF EXISTS trips;

CREATE TABLE `trips` (
    route_id VARCHAR(255),
    service_id VARCHAR(255),
    trip_id VARCHAR(255),
    trip_headsign VARCHAR(255),
    trip_short_name VARCHAR(255),
    direction_id VARCHAR(255),
    block_id VARCHAR(255),
    shape_id VARCHAR(255),
    wheelchair_accessible VARCHAR(255),
    bikes_allowed VARCHAR(255),
    route_variant VARCHAR(255)
);

LOAD DATA LOCAL INFILE 'data/agency.txt' INTO TABLE agency FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'data/calendar_dates.txt' INTO TABLE calendar_dates FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'data/calendar.txt' INTO TABLE calendar FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'data/routes.txt' INTO TABLE routes FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'data/stop_times.txt' INTO TABLE stop_times FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'data/stops.txt' INTO TABLE stops FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'data/trips.txt' INTO TABLE trips FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES;
