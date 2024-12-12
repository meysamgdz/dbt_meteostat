WITH routes AS (
			SELECT origin
					, dest
					, count(*) AS n_flights_per_route
					, count(DISTINCT tail_number) AS arr_tail
					, count(DISTINCT airline) AS nunique_airlines
					, avg(actual_elapsed_time) AS avg_elapsed_time
					, avg(arr_delay) AS avg_delay
					, max(arr_delay) AS max_arr_delay
					, max(dep_delay) AS max_dep_delay
					, min(arr_delay) AS min_arr_delay
					, min(dep_delay) AS min_dep_delay
					, sum(cancelled) AS cancelled_tot
					, sum(diverted) AS diverted_tot
			FROM {{ref("prep_flights")}} f 
			GROUP BY origin, dest
), routes_join_origin AS (
					SELECT 
						a.country AS origin_country
						, a.city AS origin_city
						, a.name AS origin_name
						, r.*
					FROM routes r
					JOIN {{ref("prep_airports")}} a 
					ON r.origin = a.faa
)
SELECT
	a.country AS dest_country
	, a.city AS dest_city
	, a.name AS dest_name
	, rjo.*
FROM routes_join_origin rjo
JOIN {{ref("prep_airports")}} a 
ON rjo.dest = a.faa
