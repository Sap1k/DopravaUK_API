USE buses_duk;

CREATE TABLE IF NOT EXISTS `delays` (
	`ID` INT(11) NOT NULL AUTO_INCREMENT,
	`line` VARCHAR(50) NOT NULL DEFAULT '0' COLLATE 'utf8mb4_general_ci',
	`trip` INT(11) NOT NULL DEFAULT '0',
	`last_stop` VARCHAR(50) NOT NULL DEFAULT '0' COLLATE 'utf8mb4_general_ci',
	`last_changed` TIMESTAMP NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
	PRIMARY KEY (`ID`) USING BTREE
)