-- Query 1 
WITH dedupes as (
SELECT *, ROW_NUMBER () OVER (PARTITION BY game_id, team_id, player_id) as row_num
from game_details
)
SELECT * FROM dedupes
WHERE row_num = 1 ; 

-- Query 2
CREATE TABLE user_devices_cumulated 
(
user_id numeric,
device_id numeric, 
event_time TEXT, 
browser_type TEXT, 
device_activity_date_list DATE[]
) 

INSERT INTO user_devices_cumulated 
with user_activity as (
SELECT e.url as url, e.user_id as user_id, e.device_id as device_id, e.event_time as event_time
FROM events as e
join users as u
on e.user_id = u.user_id
), 
user_activity_extended as (
SELECT d.url, d.user_id as user_id, d.device_id as device_id, d.event_time as event_time, b.browser_type as browser_type
from user_activity as d
join devices as b 
on b.device_id = d.device_id
)
SELECT 
    user_id,
    device_id,
    MIN(event_time) AS event_time,
    browser_type,
    ARRAY_AGG(DISTINCT DATE(event_time)) AS device_activity_date_list
FROM user_activity_extended
GROUP BY user_id, device_id, browser_type

SELECT * FROM user_devices_cumulated; 

-- Query 3
CREATE TABLE event_devices_cumulated
(
user_id numeric,
device_id numeric, 
browser_type TEXT,
host TEXT, 
referrer TEXT, 
event_time TEXT, 
device_activity_date_list DATE[]
) 

INSERT INTO event_devices_cumulated
with user_activity as (
SELECT e.url as url, e.referrer as referrer, e.host as host, e.user_id as user_id, e.device_id as device_id, e.event_time as event_time
FROM events as e
join users as u
on e.user_id = u.user_id
), 
device_activity_extended as (
SELECT d.url as url, d.referrer as referrer, d.host as host, d.user_id as user_id, d.device_id as device_id, d.event_time as event_time, b.browser_type as browser_type
from user_activity as d
join devices as b 
on b.device_id = d.device_id
)
SELECT 
    user_id,
	device_id,
	browser_type,
	host,
	referrer,  
    MIN(event_time) AS event_time,
    ARRAY_AGG(DISTINCT DATE(event_time)) AS device_activity_date_list
FROM device_activity_extended
GROUP BY user_id, referrer, host, device_id, browser_type
ORDER BY referrer, user_id; 

SELECT * FROM event_devices_cumulated; 


-- Query 4

ALTER TABLE user_devices_cumulated
ADD COLUMN device_activity_day INT[],
ADD COLUMN device_activity_month INT[],
ADD COLUMN device_activity_year INT[];

-- Updating the new columns with the respective parts of each date
UPDATE user_devices_cumulated; 
SET 
    device_activity_day = ARRAY(
        SELECT EXTRACT(DAY FROM date_column)::INT
        FROM unnest(device_activity_date_list) AS date_column
    ),
    device_activity_month = ARRAY(
        SELECT EXTRACT(MONTH FROM date_column)::INT
        FROM unnest(device_activity_date_list) AS date_column
    ),
    device_activity_year = ARRAY(
        SELECT EXTRACT(YEAR FROM date_column)::INT
        FROM unnest(device_activity_date_list) AS date_column
    );

SELECT * FROM user_devices_cumulated;


ALTER TABLE event_devices_cumulated
ADD COLUMN device_activity_day INT[],
ADD COLUMN device_activity_month INT[],
ADD COLUMN device_activity_year INT[];

UPDATE event_devices_cumulated
SET 
    device_activity_day = ARRAY(
        SELECT EXTRACT(DAY FROM date_column)::INT
        FROM unnest(device_activity_date_list) AS date_column
    ),
    device_activity_month = ARRAY(
        SELECT EXTRACT(MONTH FROM date_column)::INT
        FROM unnest(device_activity_date_list) AS date_column
    ),
    device_activity_year = ARRAY(
        SELECT EXTRACT(YEAR FROM date_column)::INT
        FROM unnest(device_activity_date_list) AS date_column
    );

	SELECT * FROM event_devices_cumulated;



-- Query 5
CREATE TABLE host_activity_datelist(
device_activity_day INTEGER[], 
device_activity_month INTEGER[],
device_activity_year INTEGER[], 
host TEXT
)


INSERT INTO host_activity_datelist(
SELECT device_activity_day, device_activity_month, device_activity_year, host 
from event_devices_cumulated
)

SELECT * FROM host_activity_datelist; 


-- Query 6 
SELECT * FROM event_devices_cumulated
WHERE host = 'www.eczachly.com'; 

CREATE TABLE incremental_host_activity_datelist (
host text, 
device_activity_date_list DATE[]
)

INSERT INTO incremental_host_activity_datelist
SELECT 
    host,
    ARRAY_AGG(DISTINCT unnest_date) AS activity_date_list
FROM (
    SELECT 
        host,
        unnest(device_activity_date_list) AS unnest_date
    FROM event_devices_cumulated
) AS unnest_data
GROUP BY host;

SELECT * FROM incremental_host_activity_datelist; 


-- Query 7 
CREATE TABLE host_activity_reduced (
month INTEGER, 
host TEXT, 
hit_array INTEGER, 
UNIQUE_VISITORS integer[]
)

