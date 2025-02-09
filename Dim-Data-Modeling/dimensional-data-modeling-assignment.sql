-- QUERY 1 
-- DDL for actors table: Create a DDL for an actors table with the following fields:

CREATE TYPE films as (
film TEXT,
votes INTEGER, 
rating REAL, 
filmid TEXT 
)

CREATE TABLE actors (
films films[],
quality_class TEXT, 
is_active boolean, 
actorid TEXT, 
actor TEXT,
current_year int
); 


INSERT INTO actors(films, quality_class, is_active, actorid, actor, current_year) 
        WITH actor_film_array AS (
            SELECT 
                ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films_array, 
				actorid, 
				actor,
				year
            FROM actor_films
            GROUP BY actor, actorid, year
        ),
        average_rating AS (
            SELECT 
    			actorid,
    			actor,
    			year,
    			AVG(rating) AS avg_rating
			FROM actor_films 
			WHERE year = (
    						SELECT MAX(year) 
    						FROM actor_films af2 
    						WHERE af2.actorid = actor_films.actorid
						  )
			GROUP BY actor, actorid, year
        ),
        actor_status AS (
          SELECT 
    		actorid,
    		actor, 
    		year,
    		CASE WHEN MAX(YEAR) OVER (PARTITION BY actorid, actor) = 2021 THEN TRUE ELSE FALSE END AS is_active
		FROM actor_films
		GROUP BY actorid, actor, year
        ), 
		last_cte as (
        SELECT 
		f.films_array AS films,
            CASE 
                WHEN r.avg_rating > 8 THEN 'star'
                WHEN r.avg_rating > 7 AND r.avg_rating <= 8 THEN 'good'
                WHEN r.avg_rating > 6 AND r.avg_rating <= 7 THEN 'average'
                ELSE 'bad'
            END AS quality_class,
            s.is_active, 
			f.actorid, 
			f.actor, 
			f.year
        FROM actor_film_array f
        JOIN average_rating r ON f.actorid = r.actorid
        JOIN actor_status s ON f.actorid = s.actorid),
		
		goes_into_actors as (
		SELECT *, row_number () OVER (PARTITION BY films, quality_class, is_active, actorid, actor, year) as row_num
		FROM last_cte
		)
		SELECT films, quality_class, is_active, actorid, actor, year
		from goes_into_actors where row_num = 1; 



SELECT * FROM actors; 

-- QUERY 2
-- Cumulative table generation query: Write a query that populates the actors table one year at a time:

INSERT INTO actors
WITH yesterday as (
SELECT * FROM actors
WHERE current_year = 1979
), 
today as (
SELECT * FROM actor_films
WHERE year = 1980
)

SELECT 
	CASE WHEN y.films IS NULL THEN ARRAY[ROW(
										t.film, 
										t.votes, 
										t.rating, 
										t.filmid
										)::films]
					WHEN t.film IS NOT NULL THEN
									y.films || ARRAY 
									  [ROW(
										t.film, 
										t.votes, 
										t.rating, 
										t.filmid)::films]
					ELSE y.films 			 
					END as film_stats, 
	y.quality_class as quality_class, 
	y.is_active as is_active,
	COALESCE(t.actorid, y.actorid) as actorid, 
	COALESCE(t.actor, y.actor) as actor, 
	COALESCE(t.year, y.current_year+1) as current_year	
FROM today t FULL OUTER JOIN yesterday y on t.actorid = y.actorid; 




with unnested as (
SELECT actor,
UNNEST(films)::films as films
FROM actors WHERE current_year = 1975)

SELECT actor, (films::films).*
from unnested; 

SELECT films[cardinality(films)] as first_year
from actors
where current_year = 1970; 


-- QUERY 3 
-- DDL for actors_history_scd table: Create a DDL for an actors_history_scd table with the following features:

-- QUERY 4 
-- Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

CREATE TABLE actors_history_scd (
actorid TEXT, 
actor TEXT,
CURRENT_YEAR INT,
quality_class TEXT, 
previous_quality_status TEXT,
is_active boolean,
previous_active_status boolean, 
start_date INTEGER, 
end_date INTEGER
)


INSERT INTO actors_history_scd (
	actorid, 
	actor, 
    current_year, 
    quality_class, 
    previous_quality_status,
    is_active, 
    previous_active_status, 
    start_date, 
    end_date)
SELECT 
	actorid,
	actor, 
	current_year, 
	quality_class, 
	LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year)	as previous_quality_status,
	is_active, 
	LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) as previous_active_status, 
	MIN(CURRENT_YEAR) OVER (PARTITION BY actor, actorid) as start_date, 	
	MAX(CURRENT_YEAR) OVER (PARTITION BY actor, actorid) as end_date
FROM actors
GROUP BY actor, CURRENT_YEAR, quality_class, is_active, actorid; 




-- QUERY 5
-- Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.

CREATE TYPE SCD_TYPE AS (
		quality_class quality_class, 
		is_active BOOLEAN, 
		start_date INTEGER, 
		end_date INTEGER
)

WITH last_year_scd AS (
   SELECT * FROM actors_history_scd
   WHERE current_year = 2020 AND end_date = 2020
),
this_year_scd AS (
   SELECT * FROM actors
   WHERE current_year = 2021
),
scd_changes AS (
   SELECT 
       ts.actorid,
       ts.actor,
       ts.current_year,
       ts.quality_class,
       ts.is_active,
       CASE 
           WHEN ls.quality_class != ts.quality_class OR 
                ls.is_active != ts.is_active 
           THEN 'CHANGED'
           ELSE 'UNCHANGED'
       END AS record_status
   FROM this_year_scd ts
   LEFT JOIN last_year_scd ls ON ts.actorid = ls.actorid
)


INSERT INTO actors_history_scd 
(actorid, actor, current_year, quality_class, is_active, start_date, end_date)
SELECT 
   actorid, 
   actor, 
   current_year, 
   quality_class, 
   is_active, 
   current_year AS start_date,
   current_year AS end_date
FROM scd_changes;


SELECT * FROM actors_history_scd ; 





