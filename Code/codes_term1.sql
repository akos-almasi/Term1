DROP SCHEMA IF EXISTS formulaone;
CREATE SCHEMA formulaone;
USE formulaone;

-- I wanted to analyze the performance of drivers in formula 1, in order to achieve this I wanted to collect relevant data that is connected to the drivers. 
-- I used 5 tables from my dataset(raceresults, races, teams, drivers and circuits).

-- First step: create tables for the analysis

CREATE TABLE circuits(
   circuitId  INTEGER  NOT NULL,
   circuitRef VARCHAR(255),
   name       VARCHAR(255),
   location   VARCHAR(255),
   country    VARCHAR(255),
   lat	VARCHAR(255),
   lng VARCHAR(255),
   alt INTEGER,
   url VARCHAR(255),
   PRIMARY KEY (circuitId)
);

CREATE TABLE drivers(
   driverId    INTEGER  NOT NULL,
   driverRef   VARCHAR(255),
   drivernumber	VARCHAR(255),
   drivercode        VARCHAR(255),
   forename    VARCHAR(255),
   surname     VARCHAR(255),
   dob         VARCHAR(255),
   drivernationality VARCHAR(255),
   url 		VARCHAR(255),
   PRIMARY KEY (driverId)
);

CREATE TABLE races(
   raceId      INTEGER  NOT NULL,
   raceyear        INTEGER  NOT NULL,
   raceround       INTEGER  NOT NULL,
   circuitId   INTEGER  NOT NULL,
   racename        VARCHAR(255),
   racedate        DATE,
   racetime        VARCHAR(255),
   url VARCHAR(255),
   fp1_date    VARCHAR(255),
   fp1_time    VARCHAR(255),
   fp2_date    VARCHAR(255),
   fp2_time    VARCHAR(255),
   fp3_date    VARCHAR(255),
   fp3_time    VARCHAR(255),
   quali_date  VARCHAR(255),
   quali_time  VARCHAR(255),
   sprint_date VARCHAR(255),
   sprint_time VARCHAR(255),
   PRIMARY KEY (raceId),
   FOREIGN KEY (circuitId) REFERENCES circuits(circuitId)
);
CREATE TABLE constructor(
   constructorId          INTEGER  NOT NULL PRIMARY KEY,
   constructorRef         VARCHAR(255) NOT NULL,
   constructorname        VARCHAR(255) NOT NULL,
   constructornationality VARCHAR(255) NOT NULL,
   url                    VARCHAR(255) NOT NULL
);

CREATE TABLE raceresults(

   resultId        INTEGER NOT NULL,
   raceId          INTEGER NOT NULL,
   driverId        INTEGER NOT NULL,
   constructorId   INTEGER NOT NULL,
   number          VARCHAR(255),
   grid            INTEGER,
   position        VARCHAR(255),
   positionText    VARCHAR(255),
   positionOrder   INTEGER,
   points          NUMERIC(4,2),
   laps            INTEGER,
   time            VARCHAR(255),
   milliseconds    VARCHAR(255),
  fastestLap      VARCHAR(255),
  resultsrank		VARCHAR(255),
  fastestLapTime  VARCHAR(255),
  fastestLapSpeed VARCHAR(255),
  statusId        INTEGER,
   PRIMARY KEY (resultId), 
   FOREIGN KEY (raceId) REFERENCES races(raceId),
   FOREIGN KEY (driverId) REFERENCES drivers(driverId),
   FOREIGN KEY (constructorId) REFERENCES constructor(constructorId)
   );

-- Second step: Load the data into the created tables using the sql files stored in the data folder.
-- In the following order: circuits, drivers, races, constructor, raceresults.
#######################################################################################################################################################################
-- Third step: Create a denormalized data structure to be able to analyze drivers
DROP PROCEDURE IF EXISTS AllF1data;

DELIMITER //
CREATE PROCEDURE AllF1data()
BEGIN

DROP TABLE IF EXISTS tablef1;

    CREATE TABLE tablef1 AS
	SELECT 
		d.driverId,
                d.forename,
                d.surname,
		rr.grid,
                rr.positionOrder,
                rr.points,
                r.raceyear,
                r.racename,
                cir.name AS circuit_name,
                cir.country,
                con.constructorname AS team
	FROM raceresults AS rr
		INNER JOIN constructor AS con
			USING (constructorId)
		INNER JOIN drivers AS d
			USING (driverId)
		INNER JOIN races AS r
			USING (raceId)
		INNER JOIN circuits AS cir
			USING (circuitId);
END //
DELIMITER ;

# In order to call our store procedure
CALL AllF1data();
# Check our datatable created
SELECT * FROM tablef1;

#######################################################################################################################################################################
-- Forth step: Since we have many races in a year it makes sense to create an ETL pipeline to make sure that my data is up to date after a race is completed.
# create a table to log
DROP TABLE IF EXISTS log;
CREATE TABLE log (log VARCHAR (255) NOT NULL);

# Create a trigger
DROP TRIGGER IF EXISTS add_race;
TRUNCATE log;

DELIMITER $$

CREATE TRIGGER add_race
AFTER INSERT ON raceresults FOR EACH ROW
BEGIN
	-- log the order number of the newley inserted order
	INSERT INTO log SELECT CONCAT('new.raceId: ', NEW.raceId);
    
    INSERT INTO tablef1
	SELECT 
		d.driverId,
                d.forename,
                d.surname,
		rr.grid,
                rr.positionOrder,
                rr.points,
                r.raceyear,
                r.racename,
                cir.name AS circuit_name,
                cir.country,
                con.constructorname AS team
        FROM raceresults AS rr
		INNER JOIN constructor AS con
			USING (constructorId)
		INNER JOIN drivers AS d
			USING (driverId)
		INNER JOIN races AS r
			USING (raceId)
		INNER JOIN circuits AS cir
			USING (circuitId)
	WHERE raceId = NEW.raceId;
END $$

DELIMITER ;
-- Activating the trigger
#In order to activate the trigger we should insert a new row into our races data
# Our last raceId is 1096 so lets create a new one with raceId 1100
INSERT INTO races VALUES (1100,2042,22,24,'','2023-11-18','13:00:00','http://en.wikipedia.org','2023-11-18','09:00:00','2023-11-18','12:00:00','2023-11-19','10:00:00','2023-11-19','13:00:00',NULL,NULL);
INSERT INTO raceresults VALUES (26000,1100,820,117,'18',10,'10','10',10,0,56,NULL,NULL,'60','19','1:24.002','187.752',131);

# Check if the trigger works
SELECT * FROM log;
SELECT * FROM tablef1 WHERE raceyear ="2042";
SELECT * FROM races WHERE raceyear ="2042";

-- Drop these  created rows
DELETE FROM formulaone.raceresults WHERE resultId=26000;
DELETE FROM formulaone.races WHERE raceyear = "2042";
SET SQL_SAFE_UPDATES = 0;
DELETE FROM formulaone.tablef1 WHERE raceyear = "2042";


##########################################################################################################################################
-- Answering the following questions with data marts
# How many 2nd places do each driver have?
DROP VIEW IF EXISTS Number_of_second_places;
CREATE VIEW `Number_of_second_places` AS
SELECT driverId, surname, forename, COUNT(positionOrder) AS 2nd_place_count
FROM tablef1
WHERE positionOrder = 2
GROUP BY driverId, surname, forename
ORDER BY 2nd_place_count DESC;

# How many seasons has each driver appeared in F1?
DROP VIEW IF EXISTS season_count;
CREATE VIEW `season_count` AS
SELECT driverId, forename, surname, COUNT(DISTINCT raceyear) AS number_of_seasons
FROM tablef1
GROUP BY driverId, forename, surname
ORDER BY number_of_seasons DESC;

# How many hungaroring victories does each driver have?
DROP VIEW IF EXISTS hungaroring_victory;
CREATE VIEW `Hungaroring_victory` AS
SELECT driverId, forename, surname, COUNT(positionOrder = 1) AS hungaroring_wins
FROM tablef1
WHERE circuit_name = 'Hungaroring' AND positionOrder = 1
GROUP BY driverId, forename, surname
ORDER BY hungaroring_wins DESC;

# How many points have each driver scored within each team during their career?
DROP VIEW IF EXISTS driverpoints_per_team;
CREATE VIEW `driverpoints_per_team` AS
SELECT driverId, forename, surname, team, SUM(points) AS points_earned
FROM tablef1
GROUP BY driverId, forename, surname, team
ORDER BY driverId;

-- Create an aggregation based on whether the drivers received points on a race or not
DROP VIEW IF EXISTS point_scorers;
CREATE VIEW `point_scorers` AS
SELECT driverId, forename, surname, positionOrder, points,
	CASE
		WHEN points = 0
			THEN 'No point'
		ELSE
			'Earned points'
	END
    AS race_outcome
FROM tablef1
ORDER BY race_outcome;

