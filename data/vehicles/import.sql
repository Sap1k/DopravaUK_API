CREATE DATABASE DUK_data;
USE DUK_data

CREATE DATABASE vehicles(
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