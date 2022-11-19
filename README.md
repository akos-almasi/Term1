# Data Engineering Term 1

A quick overview what I did in the Term 1 project.

Tablef1 datawarehouse have been created by joining five tables through stored procedures, which is there to help us analyze the performance of the drivers. I created an ETL pipeline for the datawarehouse and for the datamarts as well.

<img width="961" alt="Screenshot 2022-11-19 at 14 16 07" src="https://user-images.githubusercontent.com/113236007/202852663-0a0e9e7c-c11f-4e32-8794-7720b7bddc5c.png">




## Dataset
I chose a formula one dataset to deliver the Term1 Project. 
The dataset can be downloaded from the following website: https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020

## Operational layer

### Create tables
I just demonstrate how I created one table in the project.
```
CREATE TABLE drivers(
   driverId    	INTEGER  NOT NULL,
   driverRef   	VARCHAR(255),
   drivernumber	VARCHAR(255),
   drivercode  	VARCHAR(255),
   forename    	VARCHAR(255),
   surname     	VARCHAR(255),
   dob         	VARCHAR(255),
   drivernationality VARCHAR(255),
   url 		VARCHAR(255),
   PRIMARY KEY (driverId)
);
```

### Load data into the tables
I created 5 sql files (data folder) from the original csvs and loaded them into the created tables in the following order: 
circuits, drivers, races, constructors, raceresults.

## Analytics
I wanted to analyze the performance of drivers in formula 1, in order to achieve this I wanted to collect relevant data that is connected to the drivers. 
I used 5 tables from the formula one dataset(results, races, constructors, drivers and circuits).


### EER diagram
I have created an EER diagram so you can have a quick look at the relational dataset.

<img width="923" alt="Screenshot 2022-11-15 at 18 03 01" src="https://user-images.githubusercontent.com/113236007/201982091-5acf9fa8-b8ee-4d94-a75b-18d92498a6d1.png">

## Analytical layer
In order to analyze the drivers I created a denormalized data structure (stored procedure).

```
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
	FROM 	raceresults AS rr
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
The appearance of the Datawarehouse:

<img width="1197" alt="Screenshot 2022-11-19 at 13 02 43" src="https://user-images.githubusercontent.com/113236007/202849931-04ee86dd-81ba-4cb9-972c-68d24ed880c4.png">






## ETL pipeline
Since we have many races in a year it makes sense to create an ETL pipeline to make sure that my denormalized table is up to date after a race is completed.

```

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

```
## Data mart
I wanted to answer 4 specific questions so we can have a better understanding about the performance of the drivers.
I created 4 data marts based on the following questions:
1. How many 2nd places do each driver have?
2. How many seasons has each driver appeared in F1?
3. How many hungaroring victories does each driver have?
4. How many points have each driver scored within each team during their career?

One example for data mart:
```
DROP VIEW IF EXISTS driverpoints_per_team ;

CREATE VIEW `driverpoints_per_team` AS
SELECT driverId, forename, surname, team, SUM(points) AS points_earned
FROM tablef1
GROUP BY driverId, forename, surname, team
ORDER BY driverId;
```

Aggregation based on whether the drivers received points on a race or not:
```
DROP VIEW IF EXISTS point_scorers;

CREATE VIEW `point_scorers` AS
SELECT raceId, driverId, forename, surname, positionOrder, points,
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

