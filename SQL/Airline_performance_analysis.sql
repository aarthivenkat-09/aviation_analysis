# ########################################################################## PHASE 1 #####################################################################################
#Create data base 
CREATE DATABASE airline_analytics;
USE airline_analytics;

#Creating a table with the default data type
CREATE TABLE flights 
(
    YEAR INT,
    MONTH INT,
    DAY INT,
    DAY_OF_WEEK INT,
    AIRLINE VARCHAR(5),
    FLIGHT_NUMBER INT,
    TAIL_NUMBER VARCHAR(15),
    ORIGIN_AIRPORT VARCHAR(10),
    DESTINATION_AIRPORT VARCHAR(10),
    SCHEDULED_DEPARTURE INT,
    DEPARTURE_TIME INT,
    DEPARTURE_DELAY INT,
    TAXI_OUT INT,
    WHEELS_OFF INT,
    SCHEDULED_TIME INT,
    ELAPSED_TIME INT,
    AIR_TIME INT,
    DISTANCE INT,
    WHEELS_ON INT,
    TAXI_IN INT,
    SCHEDULED_ARRIVAL INT,
    ARRIVAL_TIME INT,
    ARRIVAL_DELAY INT,
    DIVERTED TINYINT,
    CANCELLED TINYINT,
    CANCELLATION_REASON CHAR(15),
    AIR_SYSTEM_DELAY INT,
    SECURITY_DELAY INT,
    AIRLINE_DELAY INT,
    LATE_AIRCRAFT_DELAY INT,
    WEATHER_DELAY INT
);

#Check if strict mode is turned on
SELECT @@sql_mode;

#Turn off strict mode for this session
SET SESSION sql_mode = '';   -- If turned off the null values are converted to 0 without external typecast

#Fining the secure file priv folder
SHOW VARIABLES LIKE 'secure_file_priv';  -- To find the secure folder to upload the split dataset

#Load the split data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/flights_12.csv'
INTO TABLE flights
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    YEAR,
    MONTH,
    DAY,
    DAY_OF_WEEK,
    AIRLINE,
    FLIGHT_NUMBER,
    TAIL_NUMBER,
    ORIGIN_AIRPORT,
    DESTINATION_AIRPORT,
    SCHEDULED_DEPARTURE,
    DEPARTURE_TIME,
    DEPARTURE_DELAY,
    TAXI_OUT,
    WHEELS_OFF,
    SCHEDULED_TIME,
    ELAPSED_TIME,
    AIR_TIME,
    DISTANCE,
    WHEELS_ON,
    TAXI_IN,
    SCHEDULED_ARRIVAL,
    ARRIVAL_TIME,
    ARRIVAL_DELAY,
    DIVERTED,
    CANCELLED,
    @CANCELLATION_REASON,
    AIR_SYSTEM_DELAY,
    SECURITY_DELAY,
    AIRLINE_DELAY,
    LATE_AIRCRAFT_DELAY,
    WEATHER_DELAY
)
SET
    CANCELLATION_REASON = IF(TRIM(@CANCELLATION_REASON)='', 'Not cancelled', TRIM(@CANCELLATION_REASON));

#Check the count after import
SELECT COUNT(*) FROM flights;
#Check the top 50 rows after import
Select * from flights limit 500;

#Create table for airlines
CREATE TABLE airlines
(
    Airline_code varchar(5),
	Airline_name varchar(100)
);

#Load airline data to table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/airlines.csv'
INTO TABLE airlines
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    AIRLINE_CODE,
    AIRLINE_NAME
);

#Check the airline data
select * from airlines;

#Create table for airports
create table airport
(
	Airport_code varchar(5),
    Airport_name varchar(255),
	City varchar(50),
	State varchar(5),
	Country varchar(5),
	Latitude double,
	Longitude double
);

#Load airline data to table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/airports.csv'
INTO TABLE airport
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    Airport_code,
    Airport_name,
    City,
    State,
    Country,
    Latitude,
    Longitude
);

#Check the airport data
select * from airport;

#create table for cancellation reason
create table cancellation
(
	Cancellation_Code varchar(2),
    Cancellation_reason varchar(25)
);

#Load cancellation details to table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Cancellation_reasons.csv'
INTO TABLE cancellation
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
	Cancellation_Code,
    Cancellation_reason
);

#Check the cancellation data 
select * from cancellation;

# ########################################################################## PHASE 2 #####################################################################################
-- ----------------------------------VIEW 1--------------------------------------------- --
CREATE OR REPLACE VIEW view_flight_base AS
SELECT
    f.YEAR,
    f.MONTH,
    f.DAY,
    f.DAY_OF_WEEK,
    f.AIRLINE,
    f.FLIGHT_NUMBER,
    f.TAIL_NUMBER,
    f.ORIGIN_AIRPORT,
    f.DESTINATION_AIRPORT,
    f.SCHEDULED_DEPARTURE,
    f.SCHEDULED_TIME,
    f.ELAPSED_TIME,
    f.SCHEDULED_ARRIVAL,
    f.DISTANCE,
    f.DIVERTED,
    f.CANCELLED,
    f.CANCELLATION_REASON,
    
    -- ================= NORMALIZED OPERATIONAL FIELDS =================
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.DEPARTURE_TIME,0) END AS DEPARTURE_TIME,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.DEPARTURE_DELAY,0) END AS DEPARTURE_DELAY,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.AIR_TIME,0) END AS AIR_TIME,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.TAXI_OUT,0) END AS TAXI_OUT,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.WHEELS_OFF,0) END AS WHEELS_OFF,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.WHEELS_ON,0) END AS WHEELS_ON,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.TAXI_IN,0) END AS TAXI_IN,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.ARRIVAL_TIME,0) END AS ARRIVAL_TIME,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.ARRIVAL_DELAY,0) END AS ARRIVAL_DELAY,

    -- ================= DELAY COMPONENTS =================
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.AIR_SYSTEM_DELAY,0) END AS AIR_SYSTEM_DELAY,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.SECURITY_DELAY,0) END AS SECURITY_DELAY,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.AIRLINE_DELAY,0) END AS AIRLINE_DELAY,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.LATE_AIRCRAFT_DELAY,0) END AS LATE_AIRCRAFT_DELAY,
    CASE WHEN f.CANCELLED = 1 THEN 0 ELSE IFNULL(f.WEATHER_DELAY,0) END AS WEATHER_DELAY,

    /* ============================       FLIGHT DATE (base calendar day)       ============================ */
    STR_TO_DATE(CONCAT(f.YEAR, '-',LPAD(f.MONTH, 2, '0'), '-',LPAD(f.DAY, 2, '0')),'%Y-%m-%d') AS FLIGHT_DATE,

    /* ============================       SCHEDULED DEPARTURE DATETIME       ============================ */
    
CASE
        -- Invalid / missing scheduled time
        WHEN f.SCHEDULED_DEPARTURE = 0 THEN NULL

        -- 2400 → midnight of next day
        WHEN f.SCHEDULED_DEPARTURE = 2400 THEN
            TIMESTAMP(STR_TO_DATE(CONCAT(f.YEAR, '-',LPAD(f.MONTH, 2, '0'), '-',LPAD(f.DAY, 2, '0')),'%Y-%m-%d') + INTERVAL 1 DAY,'00:00:00')
            
        -- Normal HHMM → same day
        ELSE
            STR_TO_DATE(CONCAT(f.YEAR,LPAD(f.MONTH, 2, '0'),LPAD(f.DAY, 2, '0'),LPAD(f.SCHEDULED_DEPARTURE, 4, '0')),'%Y%m%d%H%i')
    END AS SCHEDULED_DEPARTURE_DT,
    
    /* ============================   ACTUAL DEPARTURE DATETIME   ============================ */
CASE
-- Cancelled
    WHEN f.CANCELLED = 1 THEN NULL

-- Invalid scheduled departure 
    WHEN f.SCHEDULED_DEPARTURE = 0 THEN NULL

-- AUTHORITATIVE: Delay-based 
    WHEN DEPARTURE_DELAY IS NOT NULL AND DEPARTURE_DELAY <> 0 AND ABS(DEPARTURE_DELAY) <= 2880 THEN DATE_ADD(
		CASE
			WHEN f.SCHEDULED_DEPARTURE = 2400 THEN TIMESTAMP(STR_TO_DATE(CONCAT(f.YEAR,'-',LPAD(f.MONTH,2,'0'),'-',LPAD(f.DAY,2,'0')),'%Y-%m-%d') + INTERVAL 1 DAY,'00:00:00')/* Base scheduled departure datetime */
			ELSE STR_TO_DATE(CONCAT(f.YEAR,LPAD(f.MONTH,2,'0'),LPAD(f.DAY,2,'0'),LPAD(f.SCHEDULED_DEPARTURE,4,'0')),'%Y%m%d%H%i')
		END,INTERVAL DEPARTURE_DELAY MINUTE)

-- Delay = 0 AND HHMM present
    WHEN DEPARTURE_DELAY = 0 AND DEPARTURE_TIME <> 0 THEN
		CASE
			WHEN DEPARTURE_TIME = 2400 THEN TIMESTAMP(STR_TO_DATE(CONCAT(f.YEAR,'-',LPAD(f.MONTH,2,'0'),'-',LPAD(f.DAY,2,'0')),'%Y-%m-%d') + INTERVAL 1 DAY,'00:00:00')
			ELSE STR_TO_DATE(CONCAT(f.YEAR,LPAD(f.MONTH,2,'0'),LPAD(f.DAY,2,'0'),LPAD(DEPARTURE_TIME,4,'0')),'%Y%m%d%H%i')
		END

-- Delay = 0 but no HHMM 
    WHEN DEPARTURE_DELAY = 0 AND DEPARTURE_TIME = 0 THEN NULL

-- Fallback: HHMM only 
    WHEN DEPARTURE_DELAY IS NULL AND DEPARTURE_TIME <> 0 THEN
		CASE
			WHEN DEPARTURE_TIME = 2400 THEN TIMESTAMP(STR_TO_DATE(CONCAT(f.YEAR,'-',LPAD(f.MONTH,2,'0'),'-',LPAD(f.DAY,2,'0')),'%Y-%m-%d') + INTERVAL 1 DAY,'00:00:00')
			ELSE STR_TO_DATE(CONCAT(f.YEAR,LPAD(f.MONTH,2,'0'),LPAD(f.DAY,2,'0'),LPAD(DEPARTURE_TIME,4,'0')),'%Y%m%d%H%i')
		END
	ELSE NULL
END AS DEPARTURE_TIME_DT,

/* ============================   SCHEDULED ARRIVAL DATETIME   ============================ */
CASE
-- Invalid scheduled arrival 
    WHEN f.SCHEDULED_ARRIVAL = 0 THEN NULL

-- 2400 → midnight of next arrival day
    WHEN f.SCHEDULED_ARRIVAL = 2400 THEN TIMESTAMP(DATE_ADD(
        CASE
/* If scheduled departure itself is 2400 → departure already next day */
			WHEN f.SCHEDULED_DEPARTURE = 2400 THEN STR_TO_DATE(CONCAT(f.YEAR,'-',LPAD(f.MONTH,2,'0'),'-',LPAD(f.DAY,2,'0')),'%Y-%m-%d') + INTERVAL 1 DAY
			ELSE STR_TO_DATE(CONCAT(f.YEAR,'-',LPAD(f.MONTH,2,'0'),'-',LPAD(f.DAY,2,'0')),'%Y-%m-%d')
		END,INTERVAL 1 DAY),'00:00:00')

-- Normal HHMM scheduled arrival 
    ELSE STR_TO_DATE(CONCAT(DATE_FORMAT(
		CASE
/* Arrival rolls over if arrival HHMM < scheduled departure HHMM */
			WHEN f.SCHEDULED_DEPARTURE NOT IN (0,2400) AND f.SCHEDULED_ARRIVAL < f.SCHEDULED_DEPARTURE THEN STR_TO_DATE(CONCAT(f.YEAR,'-',LPAD(f.MONTH,2,'0'),'-',LPAD(f.DAY,2,'0')),'%Y-%m-%d') + INTERVAL 1 DAY
			ELSE STR_TO_DATE(CONCAT(f.YEAR,'-',LPAD(f.MONTH,2,'0'),'-',LPAD(f.DAY,2,'0')),'%Y-%m-%d')
		END,'%Y%m%d'),LPAD(f.SCHEDULED_ARRIVAL,4,'0')),'%Y%m%d%H%i')
END AS SCHEDULED_ARRIVAL_DT
FROM flights f;

select COUNT(*) FROM view_flight_base;

SELECT COUNT(*) FROM view_flight_base WHERE CANCELLED = 1 AND ( DEPARTURE_TIME <> 0 OR ARRIVAL_TIME <> 0 OR AIR_TIME <> 0);

-- ----------------------------------VIEW 2--------------------------------------------- --
CREATE OR REPLACE VIEW view_flight_mid AS
SELECT
    b.*,
/* ============================   ACTUAL ARRIVAL DT   ============================ */
CASE
    -- Cancelled flight
    WHEN b.CANCELLED = 1 THEN NULL

    -- Invalid scheduled arrival
    WHEN b.SCHEDULED_ARRIVAL = 0 THEN NULL

	-- Delay-driven (authoritative)
    WHEN b.ARRIVAL_DELAY IS NOT NULL AND b.ARRIVAL_DELAY <> 0 AND ABS(b.ARRIVAL_DELAY) <= 2880 AND DEPARTURE_TIME_DT IS NOT NULL THEN DATE_ADD( DEPARTURE_TIME_DT, INTERVAL (b.AIR_TIME + b.TAXI_IN) MINUTE)

    -- Delay = 0, on-time 
    WHEN b.ARRIVAL_DELAY = 0 AND b.ARRIVAL_TIME <> 0 AND DEPARTURE_TIME_DT IS NOT NULL THEN DATE_ADD(DEPARTURE_TIME_DT,INTERVAL (b.AIR_TIME + b.TAXI_IN) MINUTE)

    -- Delay = 0 but no arrival HHMM
    WHEN b.ARRIVAL_DELAY = 0 AND b.ARRIVAL_TIME = 0 THEN NULL

    -- HHMM fallback 
    WHEN b.ARRIVAL_DELAY IS NULL AND b.ARRIVAL_TIME <> 0 AND DEPARTURE_TIME_DT IS NOT NULL THEN
		CASE
-- 2400 → midnight next day
			WHEN b.ARRIVAL_TIME = 2400 THEN TIMESTAMP(DATE_ADD(DATE(DEPARTURE_TIME_DT), INTERVAL 1 DAY),'00:00:00')
-- Normal HHMM
            ELSE STR_TO_DATE(CONCAT(DATE_FORMAT(
				CASE
					WHEN b.ARRIVAL_TIME < b.DEPARTURE_TIME THEN DATE_ADD(DEPARTURE_TIME_DT, INTERVAL 1 DAY)
					ELSE DATE(DEPARTURE_TIME_DT)
				END,'%Y%m%d'),LPAD(b.ARRIVAL_TIME,4,'0')),'%Y%m%d%H%i')
		END

    ELSE NULL
END AS ARRIVAL_TIME_DT,


/* ============================   WHEELS OFF DT   ============================ */
CASE
    WHEN b.DEPARTURE_TIME_DT IS NULL THEN NULL

-- Taxi-out authoritative 
    WHEN b.TAXI_OUT IS NOT NULL AND b.TAXI_OUT BETWEEN 0 AND 300 THEN DATE_ADD(b.DEPARTURE_TIME_DT, INTERVAL b.TAXI_OUT MINUTE)

-- HHMM fallback 
    WHEN b.WHEELS_OFF <> 0 THEN
        CASE
-- 2400 → midnight next day
            WHEN b.WHEELS_OFF = 2400 THEN TIMESTAMP(DATE_ADD(DATE(b.DEPARTURE_TIME_DT), INTERVAL 1 DAY),'00:00:00')
-- Normal HHMM
            ELSE STR_TO_DATE(CONCAT(DATE_FORMAT(
				CASE
                    WHEN b.WHEELS_OFF < b.DEPARTURE_TIME THEN DATE_ADD(b.DEPARTURE_TIME_DT, INTERVAL 1 DAY)
					ELSE DATE(b.DEPARTURE_TIME_DT)
				END,'%Y%m%d'),LPAD(b.WHEELS_OFF,4,'0')),'%Y%m%d%H%i')
        END

    ELSE NULL
END AS WHEELS_OFF_DT
FROM view_flight_base b;

select COUNT(*) FROM view_flight_mid;

-- ----------------------------------VIEW 3--------------------------------------------- --
CREATE OR REPLACE VIEW view_flight_top AS
SELECT
    m.*,
/* ============================   WHEELS ON DT   ============================ */
 CASE
    WHEN m.WHEELS_OFF_DT IS NULL THEN NULL
-- Authoritative: air-time based 
    WHEN m.AIR_TIME IS NOT NULL AND m.AIR_TIME BETWEEN 1 AND 1440 THEN DATE_ADD(m.WHEELS_OFF_DT, INTERVAL m.AIR_TIME MINUTE)
-- HHMM fallback
    WHEN m.WHEELS_ON <> 0 THEN
CASE
-- 2400 → midnight next day
	WHEN m.WHEELS_ON = 2400 THEN TIMESTAMP(DATE_ADD(DATE(m.WHEELS_OFF_DT), INTERVAL 1 DAY),'00:00:00')
-- Normal HHMM
	ELSE STR_TO_DATE(CONCAT(DATE_FORMAT(
		CASE
			WHEN m.WHEELS_ON < m.WHEELS_OFF THEN DATE_ADD(m.WHEELS_OFF_DT, INTERVAL 1 DAY)
            ELSE DATE(m.WHEELS_OFF_DT)
		END,'%Y%m%d'),LPAD(m.WHEELS_ON,4,'0')),'%Y%m%d%H%i')
END
    ELSE NULL
END AS WHEELS_ON_DT,
   
-- OPERATIONAL DELAY COLUMN ADDITION
CASE
    WHEN IFNULL(m.ARRIVAL_DELAY, 0) > 0 THEN 
    GREATEST(IFNULL(m.ARRIVAL_DELAY, 0)- (IFNULL(m.AIR_SYSTEM_DELAY, 0)+ IFNULL(m.SECURITY_DELAY, 0)+ IFNULL(m.AIRLINE_DELAY, 0)+ IFNULL(m.LATE_AIRCRAFT_DELAY, 0)+ IFNULL(m.WEATHER_DELAY, 0)),0)
    ELSE 0
END AS OPERATIONAL_DELAY
FROM view_flight_mid m;

SELECT COUNT(*) FROM view_flight_top;

-- -----------------------------------VIEW 4---------------------------------------- --
CREATE OR REPLACE VIEW view_flight_analytics AS
SELECT
    f.*,

    /* =========================       Airline Dimension       ========================= */
    al.AIRLINE_NAME,

    /* =========================       Origin Airport Dimension       ========================= */
    ao.AIRPORT_NAME   AS ORIGIN_AIRPORT_NAME,
    ao.CITY           AS ORIGIN_CITY,
    ao.STATE          AS ORIGIN_STATE,
    ao.COUNTRY        AS ORIGIN_COUNTRY,
    ao.LATITUDE       AS ORIGIN_LATITUDE,
    ao.LONGITUDE      AS ORIGIN_LONGITUDE,

    /* =========================       Destination Airport Dimension       ========================= */
    ad.AIRPORT_NAME   AS DEST_AIRPORT_NAME,
    ad.CITY           AS DEST_CITY,
    ad.STATE          AS DEST_STATE,
    ad.COUNTRY        AS DEST_COUNTRY,
    ad.LATITUDE       AS DEST_LATITUDE,
    ad.LONGITUDE      AS DEST_LONGITUDE,

    /* =========================       Cancellation Reason Mapping       ========================= */
    CASE
        WHEN f.CANCELLATION_REASON IN ('A','B','C','D') THEN cr.Cancellation_reason
        ELSE f.CANCELLATION_REASON
    END AS CANCELLATION_REASON_DESC,

-- MINUTES → TIME (HH:MM:SS)
    SEC_TO_TIME(IFNULL(f.DEPARTURE_DELAY, 0) * 60) 			AS DEPARTURE_DELAY_TS,
    SEC_TO_TIME(IFNULL(f.TAXI_OUT, 0) * 60)        			AS TAXI_OUT_TS,
    SEC_TO_TIME(IFNULL(f.AIR_TIME, 0) * 60)       			AS AIR_TIME_TS,
    SEC_TO_TIME(IFNULL(f.TAXI_IN, 0) * 60)        			AS TAXI_IN_TS,
    SEC_TO_TIME(IFNULL(f.ARRIVAL_DELAY, 0) * 60)  			AS ARRIVAL_DELAY_TS,
	SEC_TO_TIME(IFNULL(f.AIR_SYSTEM_DELAY, 0) * 60) 		AS AIR_SYSTEM_DELAY_TIME,
	SEC_TO_TIME(IFNULL(f.SECURITY_DELAY, 0) * 60) 			AS SECURITY_DELAY_TIME,
	SEC_TO_TIME(IFNULL(f.AIRLINE_DELAY, 0) * 60) 			AS AIRLINE_DELAY_TIME,
	SEC_TO_TIME(IFNULL(f.LATE_AIRCRAFT_DELAY, 0) * 60) 		AS LATE_AIRCRAFT_DELAY_TIME,
	SEC_TO_TIME(IFNULL(f.WEATHER_DELAY, 0) * 60) 			AS WEATHER_DELAY_TIME,
	SEC_TO_TIME(IFNULL(f.OPERATIONAL_DELAY, 0) * 60) 		AS OPERATIONAL_DELAY_TIME,
    
-- WEEKDAY AS NAMES
CASE f.DAY_OF_WEEK
    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday'
    WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday'
    WHEN 5 THEN 'Friday'
    WHEN 6 THEN 'Saturday'
    WHEN 7 THEN 'Sunday'
    ELSE 'Unknown'
END AS DAY_OF_WEEK_NAME
    
FROM view_flight_top f
LEFT JOIN airlines al ON f.AIRLINE = al.AIRLINE_CODE
LEFT JOIN airport ao ON f.ORIGIN_AIRPORT = ao.AIRPORT_CODE
LEFT JOIN airport ad ON f.DESTINATION_AIRPORT = ad.AIRPORT_CODE
LEFT JOIN cancellation cr ON f.CANCELLATION_REASON = cr.CANCELLATION_CODE;
    
SELECT * FROM view_flight_analytics LIMIT 5;

select count(*) from view_flight_analytics;

-- ----------------------Creating materialized tables, adding indexes --------------------------------

-- Create table structure alone
CREATE TABLE flight_analytics_cleaned AS SELECT * FROM view_flight_analytics WHERE 1 = 0;

-- Insert data to table
INSERT INTO flight_analytics_cleaned (
    YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER,
    ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME,
    DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME,
    DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY,
    DIVERTED, CANCELLED, CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY,
    AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY,FLIGHT_DATE,SCHEDULED_DEPARTURE_DT,
    DEPARTURE_TIME_DT,SCHEDULED_ARRIVAL_DT, ARRIVAL_TIME_DT, WHEELS_OFF_DT, WHEELS_ON_DT,OPERATIONAL_DELAY,
    AIRLINE_NAME,ORIGIN_AIRPORT_NAME,ORIGIN_CITY,ORIGIN_STATE,ORIGIN_COUNTRY,ORIGIN_LATITUDE,ORIGIN_LONGITUDE,
    DEST_AIRPORT_NAME,DEST_CITY,DEST_STATE,DEST_COUNTRY,DEST_LATITUDE,DEST_LONGITUDE,CANCELLATION_REASON_DESC,
    DEPARTURE_DELAY_TS,TAXI_OUT_TS,AIR_TIME_TS,TAXI_IN_TS,ARRIVAL_DELAY_TS,AIR_SYSTEM_DELAY_TIME,
    SECURITY_DELAY_TIME,AIRLINE_DELAY_TIME,LATE_AIRCRAFT_DELAY_TIME,WEATHER_DELAY_TIME,OPERATIONAL_DELAY_TIME,DAY_OF_WEEK_NAME
)
SELECT
    YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER,
    ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE, DEPARTURE_TIME,
    DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME,
    DISTANCE, WHEELS_ON, TAXI_IN, SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY,
    DIVERTED, CANCELLED, CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY,
    AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY,FLIGHT_DATE,SCHEDULED_DEPARTURE_DT,
    DEPARTURE_TIME_DT,SCHEDULED_ARRIVAL_DT, ARRIVAL_TIME_DT, WHEELS_OFF_DT, WHEELS_ON_DT,OPERATIONAL_DELAY,
    AIRLINE_NAME,ORIGIN_AIRPORT_NAME,ORIGIN_CITY,ORIGIN_STATE,ORIGIN_COUNTRY,ORIGIN_LATITUDE,ORIGIN_LONGITUDE,
    DEST_AIRPORT_NAME,DEST_CITY,DEST_STATE,DEST_COUNTRY,DEST_LATITUDE,DEST_LONGITUDE,CANCELLATION_REASON_DESC,
    DEPARTURE_DELAY_TS,TAXI_OUT_TS,AIR_TIME_TS,TAXI_IN_TS,ARRIVAL_DELAY_TS,AIR_SYSTEM_DELAY_TIME,
    SECURITY_DELAY_TIME,AIRLINE_DELAY_TIME,LATE_AIRCRAFT_DELAY_TIME,WEATHER_DELAY_TIME,OPERATIONAL_DELAY_TIME,DAY_OF_WEEK_NAME
FROM view_flight_analytics LIMIT 5800000, 200000;  -- batch size

select count(*) from flight_analytics_cleaned;
    
# ########################################################################## PHASE 3 #####################################################################################
-- --------------------------------------------------Task 1------------------------------------------------------
create table 1_summary_overall_flight_health(
    total_flights BIGINT,
    cancelled_flights BIGINT,
    operated_flights BIGINT,
    ontime_flights BIGINT,
    delayed_flights BIGINT
);

INSERT INTO 1_summary_overall_flight_health
SELECT
    COUNT(*) AS total_flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS operated_flights,
    SUM(CASE WHEN CANCELLED = 0 AND ARRIVAL_DELAY < 15 THEN 1 ELSE 0 END) AS ontime_flights,
    SUM(CASE WHEN CANCELLED = 0 AND ARRIVAL_DELAY >= 15 THEN 1 ELSE 0 END) AS delayed_flights
FROM flight_analytics_cleaned;

select * from 1_summary_overall_flight_health;	

-- --------------------------------------------------Task 2------------------------------------------------------
CREATE TABLE 2_summary_airline_performance (
    airline_code               VARCHAR(10),
    airline_name               VARCHAR(100),
    month                       TINYINT,
    total_flights               INT,
    operated_flights            INT,
    cancelled_flights           INT,
    ontime_flights              INT,
    avg_arrival_delay_mins_int  INT,
    avg_arrival_delay_time      TIME,
    PRIMARY KEY (airline_code, month)
);

INSERT INTO 2_summary_airline_performance
SELECT	
    AIRLINE AS airline_code,
    AIRLINE_NAME AS airline_name,
    MONTH as month,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS operated_flights,  /* Operated flights */
	SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,  /* Cancelled flights */
	SUM(CASE WHEN CANCELLED = 0 AND arrival_delay_ts < '00:15:00' THEN 1 ELSE 0 END) AS ontime_flights, /* On-time flights (< 15 mins, early arrivals INCLUDED) */
    ROUND( AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_mins_int,  /* ✅ TRUE aviation average arrival delay (includes negatives) */
	SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(arrival_delay_ts)END),0)) AS avg_arrival_delay_time    
FROM flight_analytics_cleaned GROUP BY AIRLINE, AIRLINE_NAME,MONTH;

SELECT * FROM 2_summary_airline_performance;

-- --------------------------------------------------Task 3------------------------------------------------------
CREATE TABLE 3_summary_airport_performance (
    origin_airport_code             VARCHAR(10),
    origin_airport_name             VARCHAR(100),
    month                           TINYINT,
    total_flights                   INT,
    cancelled_flights               INT,
    cancellation_rate_pct           DECIMAL(5,2),
    avg_departure_delay_mins_int    INT,
    avg_departure_delay_time        TIME,
    avg_arrival_delay_mins_int      INT,
    avg_arrival_delay_time          TIME,
    avg_taxi_out 					INT,
    avg_taxi_in						INT,
    PRIMARY KEY (origin_airport_code, month)
);

INSERT INTO 3_summary_airport_performance
SELECT
    ORIGIN_AIRPORT AS origin_airport_code,
	COALESCE(ORIGIN_AIRPORT_NAME,CONCAT('Airport Code: ', ORIGIN_AIRPORT)) AS origin_airport_name,
    MONTH AS month,
    COUNT(*) AS total_flights,    
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,  /* Cancelled flights */
	ROUND(SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),2) AS cancellation_rate_pct,  /* Cancellation rate (%) */
	ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_departure_delay_int,  /* Avg DEPARTURE delay in minutes (INT, negatives included) */
	SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(departure_delay_ts)END),0)) AS avg_departure_delay_time,  /* Avg DEPARTURE delay as TIME */
	ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_int,  /* Avg ARRIVAL delay in minutes (INT, negatives included) */
	SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(arrival_delay_ts)END),0)) AS avg_arrival_delay_time,  /* Avg ARRIVAL delay as TIME */
	ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TAXI_OUT END),0) AS avg_taxi_out,  
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TAXI_IN END),0) AS avg_taxi_in
FROM flight_analytics_cleaned GROUP BY origin_airport_code, ORIGIN_AIRPORT_NAME,MONTH;

select * from 3_summary_airport_performance;

CREATE TABLE 3_summary_dest_airport_performance (
    destination_airport_code        VARCHAR(10),
    destination_airport_name        VARCHAR(100),
    month                           TINYINT,
    total_flights                   INT,
    cancelled_flights               INT,
    cancellation_rate_pct           DECIMAL(5,2),
    avg_departure_delay_mins_int    INT,
    avg_departure_delay_time        TIME,
    avg_arrival_delay_mins_int      INT,
    avg_arrival_delay_time          TIME,
    avg_taxi_out 					INT,
    avg_taxi_in						INT,
    PRIMARY KEY (destination_airport_code, month)
);

INSERT INTO 3_summary_dest_airport_performance
SELECT
    DESTINATION_AIRPORT AS destination_airport_code,
	COALESCE(DEST_AIRPORT_NAME,CONCAT('Airport Code: ', DESTINATION_AIRPORT)) AS destination_airport_name,
    MONTH AS month,
    COUNT(*) AS total_flights,    
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,  /* Cancelled flights */
	ROUND(SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),2) AS cancellation_rate_pct,  /* Cancellation rate (%) */
	ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_departure_delay_int,  /* Avg DEPARTURE delay in minutes (INT, negatives included) */
	SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(departure_delay_ts)END),0)) AS avg_departure_delay_time,  /* Avg DEPARTURE delay as TIME */
	ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_int,  /* Avg ARRIVAL delay in minutes (INT, negatives included) */
	SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(arrival_delay_ts)END),0)) AS avg_arrival_delay_time,  /* Avg ARRIVAL delay as TIME */
	ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TAXI_OUT END),0) AS avg_taxi_out,  
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TAXI_IN END),0) AS avg_taxi_in
FROM flight_analytics_cleaned GROUP BY destination_airport_code, destination_airport_name,MONTH;

select * from 3_summary_dest_airport_performance;


-- --------------------------------------------------Task 4------------------------------------------------------
CREATE TABLE 4_summary_dep_vs_arr_delay (
    total_flights               INT,
    avg_departure_delay_int 	INT,
    avg_departure_delay_time    TIME,
    avg_arrival_delay_int   	INT,
    avg_arrival_delay_time      TIME,
    delay_difference_int    	INT,
    delay_difference_time       TIME
);

INSERT INTO 4_summary_dep_vs_arr_delay
SELECT
    COUNT(*) AS total_flights,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_departure_delay_mins_int,  /* Avg Departure Delay (minutes, signed, includes early departures) */
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(departure_delay_ts)END),0)) AS avg_departure_delay_time,   /* Avg Departure Delay (TIME) */
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_mins_int,  /* Avg Arrival Delay (minutes, signed, includes early arrivals) */
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(arrival_delay_ts)END),0)) AS avg_arrival_delay_time,  /* Avg Arrival Delay (TIME) */
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY - DEPARTURE_DELAY END),0) AS delay_difference_mins_int,  /* Delay difference: Arrival - Departure */
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(arrival_delay_ts)  - TIME_TO_SEC(departure_delay_ts)END),0)) AS delay_difference_time
FROM flight_analytics_cleaned WHERE CANCELLED = 0;

select * from 4_summary_dep_vs_arr_delay;

-- --------------------------------------------------Task 5------------------------------------------------------
CREATE TABLE 5_summary_delay_type AS
SELECT 
    delay_type,
    total_delay_mins_int,
    ROUND(total_delay_mins_int / 60, 2) AS total_delay_hours,
    ROUND(total_delay_mins_int / 1440, 2) AS total_delay_days,
    ROUND(100 * total_delay_mins_int / SUM(total_delay_mins_int) OVER (), 2) AS delay_pct
FROM
(
    SELECT 'Airline Delay' AS delay_type, SUM(AIRLINE_DELAY) AS total_delay_mins_int
    FROM flight_analytics_cleaned WHERE CANCELLED = 0
    UNION ALL
    SELECT 'Weather Delay', SUM(WEATHER_DELAY) 
    FROM flight_analytics_cleaned WHERE CANCELLED = 0
    UNION ALL
    SELECT 'Air System Delay', SUM(AIR_SYSTEM_DELAY) 
    FROM flight_analytics_cleaned WHERE CANCELLED = 0
    UNION ALL
    SELECT 'Security Delay', SUM(SECURITY_DELAY) 
    FROM flight_analytics_cleaned WHERE CANCELLED = 0
    UNION ALL
    SELECT 'Late Aircraft Delay', SUM(LATE_AIRCRAFT_DELAY) 
    FROM flight_analytics_cleaned WHERE CANCELLED = 0
    UNION ALL
    SELECT 'Operational Delay', SUM(OPERATIONAL_DELAY) 
    FROM flight_analytics_cleaned WHERE CANCELLED = 0
) AS t;

select * from 5_summary_delay_type;

-- --------------------------------------------------Task 6------------------------------------------------------
CREATE TABLE 6_summary_cancellation_reason (
    cancellation_code   CHAR(1),
    cancellation_reason        VARCHAR(50),
    cancelled_flights          INT,
    cancellation_pct           DECIMAL(5,2)
);

INSERT INTO 6_summary_cancellation_reason
SELECT
    CANCELLATION_REASON AS cancellation_code,
    MAX(CANCELLATION_REASON_DESC) AS cancellation_reason,
    COUNT(*) AS cancelled_flights,
    ROUND(COUNT(*) * 100.0 /SUM(COUNT(*)) OVER (),2) AS cancellation_pct
FROM flight_analytics_cleaned WHERE CANCELLED = 1 GROUP BY CANCELLATION_REASON;

SELECT * FROM 6_summary_cancellation_reason;

-- --------------------------------------------------Task 7------------------------------------------------------
CREATE TABLE 7_summary_time_of_day_delay (
    time_of_day                     VARCHAR(20),
    total_flights                   INT,
    operated_flights                INT,
    /* Departure delay – Airport efficiency */
    avg_departure_delay_int    		INT,
    avg_departure_delay_time        TIME,
    /* Arrival delay – Customer experience */
    avg_arrival_delay_int    		INT,
    avg_arrival_delay_time          TIME
);

INSERT INTO 7_summary_time_of_day_delay
SELECT
    CASE
        WHEN dep_hour BETWEEN 0 AND 5  THEN 'Night'
        WHEN dep_hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN dep_hour BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN dep_hour BETWEEN 18 AND 23 THEN 'Evening'
    END AS time_of_day,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS operated_flights,
    /* ================= Departure Delay ================= */
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_departure_delay_int,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(departure_delay_ts)END),0)) AS avg_departure_delay_time,
    /* ================= Arrival Delay ================= */
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_int,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(arrival_delay_ts)END),0)) AS avg_arrival_delay_time
FROM (SELECT *,HOUR(SCHEDULED_DEPARTURE_DT) AS dep_hour FROM flight_analytics_cleaned) f GROUP BY time_of_day;

SELECT * FROM 7_summary_time_of_day_delay;

-- --------------------------------------------------Task 8------------------------------------------------------
CREATE TABLE 8_summary_day_of_week_pattern (
    day_of_week_num           TINYINT,
    day_of_week_name          VARCHAR(15),
    total_flights             INT,
    cancelled_flights         INT,
    day_cancellation_pct      DECIMAL(5,2),
    avg_departure_delay_mins  INT,
    avg_departure_delay_time  TIME,
    avg_arrival_delay_mins    INT,
    avg_arrival_delay_time   TIME,
    PRIMARY KEY (day_of_week_num)
);

INSERT INTO 8_summary_day_of_week_pattern
SELECT
    DAY_OF_WEEK AS day_of_week_num,
    DAY_OF_WEEK_NAME AS day_of_week_name,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    ROUND(SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0/ COUNT(*),2) AS day_cancellation_pct,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_departure_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(DEPARTURE_DELAY_TS) END),0)) AS avg_departure_delay_time,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(ARRIVAL_DELAY_TS) END),0)) AS avg_arrival_delay_time
FROM flight_analytics_cleaned GROUP BY DAY_OF_WEEK, DAY_OF_WEEK_NAME;

select * from 8_summary_day_of_week_pattern;

CREATE TABLE 8_summary_day_of_week_cancellation (
    day_of_week_num        TINYINT,
    day_of_week_name       VARCHAR(15),
    cancellation_code      CHAR(1),
    cancellation_reason    VARCHAR(50),
    cancelled_flights      INT,
    cancellation_pct       DECIMAL(5,2),
    PRIMARY KEY (day_of_week_num, cancellation_code)
);

INSERT INTO 8_summary_day_of_week_cancellation
SELECT
    DAY_OF_WEEK               AS day_of_week_num,
    DAY_OF_WEEK_NAME          AS day_of_week_name,
    CANCELLATION_REASON       AS cancellation_code,
    CANCELLATION_REASON_DESC  AS cancellation_reason,
    COUNT(*) AS cancelled_flights,
    ROUND(COUNT(*) * 100.0 /SUM(COUNT(*)) OVER (PARTITION BY DAY_OF_WEEK),2) AS cancellation_pct
FROM flight_analytics_cleaned WHERE CANCELLED = 1 GROUP BY DAY_OF_WEEK,DAY_OF_WEEK_NAME,CANCELLATION_REASON,CANCELLATION_REASON_DESC;

select * from 8_summary_day_of_week_cancellation;

-- --------------------------------------------------Task 9------------------------------------------------------
CREATE TABLE 9_summary_monthly_pattern(
    month_num                     TINYINT,
    month_name                    VARCHAR(15),
    total_flights                 INT,
    cancelled_flights             INT,
    cancellation_pct              DECIMAL(5,2),
    avg_departure_delay_mins      INT,
    avg_departure_delay_time      TIME,
    avg_arrival_delay_mins        INT,
    avg_arrival_delay_time        TIME,
    season 						  VARCHAR(20),
    PRIMARY KEY (month_num)
);

INSERT INTO 9_summary_monthly_pattern
SELECT
    MONTH AS month_num,
    MONTHNAME(MAKEDATE(2000, 1) + INTERVAL (MONTH - 1) MONTH) AS month_name,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    ROUND(SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0/ COUNT(*),2) AS cancellation_pct,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_departure_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(DEPARTURE_DELAY_TS) END),0)) AS avg_departure_delay_time,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(ARRIVAL_DELAY_TS) END),0)) AS avg_arrival_delay_time,
    CASE
        WHEN MONTH IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH IN (9, 10, 11) THEN 'Fall'
    END as season
FROM flight_analytics_cleaned GROUP BY MONTH;

select * from 9_summary_monthly_pattern;

-- --------------------------------------------------Task 10------------------------------------------------------
CREATE TABLE 10_summary_route_risk (
    origin_airport VARCHAR(10),
    origin_airport_name VARCHAR(100),
    destination_airport VARCHAR(10),
    destination_airport_name VARCHAR(100),
    total_flights INT,
    cancelled_flights INT,
    cancellation_pct DECIMAL(5,2),
    avg_departure_delay_mins INT,
    avg_departure_delay_time TIME,
    avg_arrival_delay_mins INT,
    avg_arrival_delay_time TIME
);

INSERT INTO 10_summary_route_risk
SELECT
    ORIGIN_AIRPORT AS origin_airport,
    COALESCE(ORIGIN_AIRPORT_NAME,CONCAT('Airport Code: ', ORIGIN_AIRPORT)) AS origin_airport_name,
    DESTINATION_AIRPORT AS destination_airport,
    COALESCE(DEST_AIRPORT_NAME,CONCAT('Airport Code: ', DESTINATION_AIRPORT)) AS destination_airport_name,
    COUNT(*) AS total_flights,
	SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    ROUND(SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0/ COUNT(*),2) AS cancellation_pct,
    COALESCE(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END), 0),0) AS avg_departure_delay_mins,
	COALESCE(SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(DEPARTURE_DELAY_TS)END),0)),'00:00:00') AS avg_departure_delay_time,
    COALESCE(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END), 0),0) AS avg_arrival_delay_mins,
	COALESCE(SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(ARRIVAL_DELAY_TS)END),0)),'00:00:00') AS avg_arrival_delay_time
FROM flight_analytics_cleaned GROUP BY ORIGIN_AIRPORT,DESTINATION_AIRPORT,ORIGIN_AIRPORT_NAME,DEST_AIRPORT_NAME;

select * from 10_summary_route_risk;
SELECT COUNT(*) FROM 10_summary_route_risk WHERE origin_airport_name LIKE 'Airport Code:%';


select count(*) from flight_analytics_cleaned where ORIGIN_AIRPORT_NAME is null;

-- --------------------------------------------------Task 11------------------------------------------------------
CREATE TABLE 11_summary_distance_delay AS
SELECT
    CASE
        WHEN DISTANCE < 500 THEN 'Short-haul (<500)'
        WHEN DISTANCE BETWEEN 500 AND 999 THEN 'Medium-haul (500–999)'
        WHEN DISTANCE BETWEEN 1000 AND 1999 THEN 'Long-haul (1000–1999)'
        ELSE 'Ultra-long (2000+)'
    END AS distance_bucket,
    COUNT(*) AS total_flights,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(ARRIVAL_DELAY_TS) END),0)) AS avg_arrival_delay_time
FROM flight_analytics_cleaned WHERE CANCELLED = 0 GROUP BY distance_bucket;

SELECT * FROM 11_summary_distance_delay;

-- --------------------------------------------------Task 12------------------------------------------------------
CREATE TABLE 12_summary_airline_reliability (
    airline_code VARCHAR(10),
    airline_name VARCHAR(100),
    total_flights INT,
    operated_flights INT,
    cancelled_flights INT,
    cancellation_pct DECIMAL(5,2),
    ontime_flights INT,
    ontime_pct DECIMAL(5,2),
    avg_arrival_delay_mins INT,
    avg_arrival_delay_time TIME,
	avg_depature_delay_mins INT,
	avg_depature_delay_time TIME,
    reliability_score DECIMAL(8,2),
    reliability_rank INT
);


INSERT INTO 12_summary_airline_reliability (
    airline_code,
    airline_name,
    total_flights,
    operated_flights,
    cancelled_flights,
    cancellation_pct,
    ontime_flights,
    ontime_pct,
    avg_arrival_delay_mins,
    avg_arrival_delay_time,
    avg_depature_delay_mins,
    avg_depature_delay_time,
    reliability_score
)
SELECT
    AIRLINE AS airline_code,
    AIRLINE_NAME AS airline_name,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS operated_flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    ROUND(SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),2) AS cancellation_pct,
    SUM(CASE WHEN CANCELLED = 0 AND ARRIVAL_DELAY < 15 THEN 1 ELSE 0 END) AS ontime_flights,
    ROUND(SUM(CASE WHEN CANCELLED = 0 AND ARRIVAL_DELAY < 15 THEN 1 ELSE 0 END) * 100.0/ NULLIF(SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END), 0),2) AS ontime_pct,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(ARRIVAL_DELAY_TS) END),0)) AS avg_arrival_delay_time,
	ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_depature_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(DEPARTURE_DELAY_TS) END),0)) AS avg_depature_delay_time,
    ROUND((
            (SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) * 0.6
            +
            (AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END)) * 0.4
        ),
        2
    ) AS reliability_score
FROM flight_analytics_cleaned GROUP BY AIRLINE,AIRLINE_NAME;


UPDATE 12_summary_airline_reliability t
JOIN (SELECT airline_code, RANK() OVER (ORDER BY reliability_score ASC) AS rnk FROM 12_summary_airline_reliability) r ON t.airline_code = r.airline_code SET t.reliability_rank = r.rnk;

SELECT * FROM 12_summary_airline_reliability;

-- --------------------------------------------------Task 13------------------------------------------------------
CREATE TABLE 13_summary_airport_congestion (
    airport_code VARCHAR(10),
    airport_name VARCHAR(150),
    total_flights INT,
    operated_flights INT,
    cancelled_flights INT,
    cancellation_pct DECIMAL(5,2),
    avg_departure_delay_mins INT,
    avg_departure_delay_time TIME,
    avg_arrival_delay_mins INT,
    avg_arrival_delay_time TIME,
    congestion_risk_score DECIMAL(8,2),
    congestion_rank INT
);

INSERT INTO 13_summary_airport_congestion(
	airport_code,
    airport_name,
    total_flights,
    operated_flights,
    cancelled_flights,
    cancellation_pct,
    avg_departure_delay_mins,
    avg_departure_delay_time,
    avg_arrival_delay_mins,
    avg_arrival_delay_time,
    congestion_risk_score
    )
SELECT
    ORIGIN_AIRPORT AS airport_code,
	COALESCE(ORIGIN_AIRPORT_NAME, CONCAT('Airport Code: ', ORIGIN_AIRPORT)) AS airport_name,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS operated_flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    ROUND(SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),2) AS cancellation_pct,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_departure_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(DEPARTURE_DELAY_TS)END),0)) AS avg_departure_delay_time,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_mins,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(ARRIVAL_DELAY_TS)END),0)) AS avg_arrival_delay_time,
    ROUND((
            (SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) * 0.5
            +
            (AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END)) * 0.3
            +
            (AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END)) * 0.2
        ),2) AS congestion_risk_score
FROM flight_analytics_cleaned
GROUP BY ORIGIN_AIRPORT,ORIGIN_AIRPORT_NAME;

UPDATE 13_summary_airport_congestion t
JOIN (SELECT airport_code, row_number() OVER (ORDER BY congestion_risk_score DESC) AS rnk FROM 13_summary_airport_congestion) r ON t.airport_code = r.airport_code SET t.congestion_rank = r.rnk;

select * from 13_summary_airport_congestion;

-- --------------------------------------------------Task 14------------------------------------------------------
CREATE TABLE 14_summary_extreme_delay_flights AS
SELECT
    FLIGHT_DATE,
    AIRLINE AS airline_code,
    AIRLINE_NAME,
    FLIGHT_NUMBER,
    ORIGIN_AIRPORT AS origin_airport_code,
    COALESCE(ORIGIN_AIRPORT_NAME,CONCAT('Airport Code: ', ORIGIN_AIRPORT)) AS origin_airport_name,
    DESTINATION_AIRPORT AS destination_airport_code,
    COALESCE(DEST_AIRPORT_NAME,CONCAT('Airport Code: ', DESTINATION_AIRPORT)) AS destination_airport_name,
    ARRIVAL_DELAY AS arrival_delay_mins,
    ARRIVAL_DELAY_TS AS arrival_delay_time
FROM flight_analytics_cleaned WHERE CANCELLED = 0 AND ARRIVAL_DELAY > 120;

select * from 14_summary_extreme_delay_flights;

CREATE TABLE 14_summary_extreme_delay_airline AS
SELECT
    AIRLINE AS airline_code,
    AIRLINE_NAME,
    COUNT(*) AS extreme_delay_flights,
    ROUND(AVG(ARRIVAL_DELAY), 0) AS avg_extreme_arrival_delay_mins,
    SEC_TO_TIME(ROUND(AVG(TIME_TO_SEC(ARRIVAL_DELAY_TS)),0)) AS avg_arrival_delay_time
FROM flight_analytics_cleaned WHERE CANCELLED = 0 AND ARRIVAL_DELAY > 120 GROUP BY AIRLINE, AIRLINE_NAME ORDER BY extreme_delay_flights DESC;

SELECT * FROM 14_summary_extreme_delay_airline;

CREATE TABLE 14_summary_extreme_delay_org_airport AS
SELECT
    ORIGIN_AIRPORT AS airport_code,
    COALESCE(ORIGIN_AIRPORT_NAME, CONCAT('Airport Code: ', ORIGIN_AIRPORT)) AS airport_name,
    COUNT(*) AS extreme_delay_flights,
    ROUND(AVG(ARRIVAL_DELAY), 0) AS avg_extreme_arrival_delay_mins,
    SEC_TO_TIME(ROUND(AVG(TIME_TO_SEC(ARRIVAL_DELAY_TS)),0)) AS avg_arrival_delay_time
FROM flight_analytics_cleaned WHERE CANCELLED = 0 AND ARRIVAL_DELAY > 120 GROUP BY ORIGIN_AIRPORT, ORIGIN_AIRPORT_NAME ORDER BY extreme_delay_flights DESC;

SELECT * FROM 14_summary_extreme_delay_org_airport;

CREATE TABLE 14_summary_extreme_delay_dest_airport AS
SELECT
    DESTINATION_AIRPORT AS airport_code,
    COALESCE(DEST_AIRPORT_NAME, CONCAT('Airport Code: ', DESTINATION_AIRPORT)) AS airport_name,
    COUNT(*) AS extreme_delay_flights,
    ROUND(AVG(ARRIVAL_DELAY), 0) AS avg_arrival_delay_mins,
	SEC_TO_TIME(ROUND(AVG(TIME_TO_SEC(ARRIVAL_DELAY_TS)),0)) AS avg_arrival_delay_time
FROM flight_analytics_cleaned
WHERE CANCELLED = 0 AND ARRIVAL_DELAY > 120 GROUP BY DESTINATION_AIRPORT,DEST_AIRPORT_NAME ORDER BY extreme_delay_flights DESC;

SELECT * FROM 14_summary_extreme_delay_dest_airport;

-- --------------------------------------------------Task 15------------------------------------------------------
CREATE TABLE 15_executive_performance_summary AS
SELECT
    COUNT(*) AS total_flights,
    SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS operated_flights,
    ROUND(SUM(CASE WHEN CANCELLED = 0 AND ARRIVAL_DELAY < 15 THEN 1 ELSE 0 END)* 100.0/ SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END),2) AS ontime_performance_pct,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN ARRIVAL_DELAY END),0) AS avg_arrival_delay_mins_int,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(ARRIVAL_DELAY_TS) END),0)) AS avg_arrival_delay_time,
    ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DEPARTURE_DELAY END),0) AS avg_departure_delay_mins_int,
    SEC_TO_TIME(ROUND(AVG(CASE WHEN CANCELLED = 0 THEN TIME_TO_SEC(DEPARTURE_DELAY_TS) END),0)) AS avg_departure_delay_time,
    ROUND(SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END)* 100.0/ COUNT(*),2) AS cancellation_rate_pct,
     (
        SELECT
            CANCELLATION_REASON_DESC
        FROM flight_analytics_cleaned f2
        WHERE f2.CANCELLED = 1
        GROUP BY CANCELLATION_REASON_DESC
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) AS top_cancellation_reason,
    (
    SELECT delay_reason
    FROM (
        SELECT 'Air System Delay' AS delay_reason, SUM(AIR_SYSTEM_DELAY) AS total_delay
        FROM flight_analytics_cleaned WHERE CANCELLED = 0
        UNION ALL
        SELECT 'Security Delay', SUM(SECURITY_DELAY) FROM flight_analytics_cleaned WHERE CANCELLED = 0
        UNION ALL
        SELECT 'Airline Delay', SUM(AIRLINE_DELAY) FROM flight_analytics_cleaned WHERE CANCELLED = 0
        UNION ALL
        SELECT 'Late Aircraft Delay', SUM(LATE_AIRCRAFT_DELAY) FROM flight_analytics_cleaned WHERE CANCELLED = 0
        UNION ALL
        SELECT 'Weather Delay', SUM(WEATHER_DELAY) FROM flight_analytics_cleaned WHERE CANCELLED = 0
        UNION ALL
        SELECT 'Operational Delay', SUM(OPERATIONAL_DELAY) FROM flight_analytics_cleaned WHERE CANCELLED = 0
    ) t
    ORDER BY total_delay DESC
    LIMIT 1
) AS top_arrival_delay_reason
FROM flight_analytics_cleaned;

SELECT * FROM 15_executive_performance_summary;