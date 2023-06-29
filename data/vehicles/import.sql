DROP DATABASE IF EXISTS buses_duk;
CREATE DATABASE buses_duk;
USE buses_duk;

CREATE TABLE vehicles (
   vhc_id INT NOT NULL UNIQUE,
   model VARCHAR(255) NOT NULL,
   agency VARCHAR(255) NOT NULL DEFAULT "neznámý",
   year_of_manufacture YEAR NOT NULL,
   low_floor BOOLEAN,
   contactless_payments BOOLEAN,
   air_conditioning BOOLEAN,
   alternate_fuel BOOLEAN,
   USB_chargers BOOLEAN,
   PRIMARY KEY (vhc_id)
);

LOAD DATA LOCAL INFILE 'vehicles.csv' INTO TABLE vehicles
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY ''
LINES TERMINATED BY '\n';

CREATE TABLE delays (
	`ID` INT(11) NOT NULL AUTO_INCREMENT,
	`vhc_id` INT(11) NOT NULL DEFAULT '0',
	`line` VARCHAR(50) NOT NULL DEFAULT '0' COLLATE 'utf8mb4_general_ci',
	`trip` INT(11) NOT NULL DEFAULT '0',
	`last_stop_id` VARCHAR(50) NOT NULL DEFAULT '0' COLLATE 'utf8mb4_general_ci',
	`delay` INT(11) NOT NULL DEFAULT '0',
	`vhc_lat` FLOAT NOT NULL DEFAULT '0',
	`vhc_lon` FLOAT NOT NULL DEFAULT '0',
	`vhc_azimuth` FLOAT NOT NULL DEFAULT '0',
	`last_changed` TIMESTAMP NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
	PRIMARY KEY (`ID`) USING BTREE
);