# Data Engineering Term 1

A quick overview what I did in the term1 project.
<img width="514" alt="Screenshot 2022-11-16 at 0 06 31" src="https://user-images.githubusercontent.com/113236007/202441783-1bcc887f-d0d7-4c7a-aae4-02facb504ea2.png">



## Dataset
I chose a formula one dataset to deliver the Term1 Project. 
The dataset can be downloaded from the following website: https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020

## Operational layer

### Create tables
```
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
```

### Load data into the tables
I created 5 sql files (which can be found in the data folder) from the original csvs and loaded them into the tables

## Analytics
I wanted to analyse the performance of drivers in formula 1, in order to achieve this I wanted to collect relevant data that is connected to the drivers. 
I used 5 tables from the formula one dataset(results, races, constructors, drivers and circuits).


### Database diagram
I have created a database diagram so you can have a quick look at the relational dataset.

<img width="923" alt="Screenshot 2022-11-15 at 18 03 01" src="https://user-images.githubusercontent.com/113236007/201982091-5acf9fa8-b8ee-4d94-a75b-18d92498a6d1.png">

## Analytical layer
In order to analyse the drivers I created a denormalized data structure (stored procedure).

```
DROP PROCEDURE IF EXISTS AllF1data;

DELIMITER //
CREATE PROCEDURE AllF1data()
BEGIN
	DROP TABLE IF EXISTS tablef1;
    CREATE TABLE tablef1 AS
	SELECT 
		rr.resultId,
                r.raceId,
                r.raceyear,
                r.racename,
                d.driverId,
                d.forename,
                d.surname,
                d.drivercode,
                d.driverRef,
                rr.grid,
                rr.positionOrder,
                rr.points,
                cir.name AS circuit_name,
                cir.country,
                con.constructorname AS team
	FROM	raceresults AS rr
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

```

## ETL pipeline
Since we have many races in a year it makes sense to create an ETL pipeline to make sure that my denormalized table is up to date after a race is completed.

```
DROP TABLE IF EXISTS log;
CREATE TABLE log (log VARCHAR (255) NOT NULL);

# Create a trigger
DROP TRIGGER IF EXISTS add_race;

DELIMITER $$

CREATE TRIGGER add_race
AFTER INSERT ON races FOR EACH ROW
BEGIN
	-- log the order number of the newley inserted order
	INSERT INTO log SELECT CONCAT('new.raceId: ', NEW.raceId);
    
    INSERT INTO tablef1
	SELECT 
		rr.resultId,
                r.raceId,
                r.raceyear,
                r.racename,
                d.driverId,
                d.forename,
                d.surname,
                d.drivercode,
                d.driverRef,
                rr.grid,
                rr.positionOrder,
                rr.points,
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
```
## Data mart
Now that we have our denormalized table and made sure that it would be up to date after a race is added to our races table, we can create views.
I created 4 data marts based on the following questions:
1. How many 2nd places do each driver have?
2. How many seasons has each driver appeared in F1?
3. How many hungaroring victories does each driver have?
4. How many points have each driver scored within each team during their career?

One example for data mart
```
DROP VIEW IF EXISTS driverpoints_per_team ;

CREATE VIEW `driverpoints_per_team` AS
SELECT driverId, forename, surname, driverRef, team, SUM(points) AS points_earned
FROM tablef1
GROUP BY driverId, forename, surname, driverRef, team
ORDER BY driverId;
```

Aggregation based on whether the drivers received points on a race or not
```
DROP VIEW IF EXISTS point_scorers;

CREATE VIEW `point_scorers` AS
SELECT raceId, driverId, forename, surname, driverRef, positionOrder, points,
	CASE
		WHEN points = 0
			THEN 'No point'
		ELSE
			'Earned points'
	END
    AS race_outcome
FROM tablef1
ORDER BY race_outcome;
```

