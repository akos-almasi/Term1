# Data Engineering Term1 Project

A quick overview what I did in the term1 project.

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

### Database diagram
I created a database diagram so we can have a quick look at our dataset.

## Analytics
I wanted to analyse the performance of drivers in formula 1, in order to achieve this I wanted to collect relevant data that is connected to the drivers. 
I used 5 tables from the formula one dataset(results, races, constructors, drivers and circuits).

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
           FROM raceresults AS rr
			INNER JOIN drivers AS d
				USING (driverId)
			INNER JOIN constructor AS con
				USING (constructorId)
			INNER JOIN races AS r
				USING (raceId)
			INNER JOIN circuits AS cir
				USING (circuitId);
END //
DELIMITER ;

```


