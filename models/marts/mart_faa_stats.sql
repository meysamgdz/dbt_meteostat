WITH departures AS (
	SELECT origin AS faa
			, count(*) AS nunique_from
			, count(sched_dep_time) AS dep_planned_tot
			, sum(cancelled) AS dep_cancelled_tot
			, sum(diverted) AS dep_diverted_tot
			, sum(CASE WHEN cancelled=0 THEN 1 ELSE 0 END) AS dep_occ_tot
			, count(DISTINCT tail_number) AS dep_tail
			, count(DISTINCT airline) AS dep_nunique_airlines
	FROM {{ref("prep_flights")}}
	GROUP BY origin
), arrivals AS (
	SELECT dest AS faa
			, count(*) AS nunique_to
			, count(sched_arr_time) AS arr_planned_tot
			, sum(cancelled) AS arr_cancelled_tot
			, sum(diverted) AS arr_diverted_tot
			, sum(CASE WHEN cancelled=0 THEN 1 ELSE 0 END) AS arr_occ_tot
			, count(DISTINCT tail_number) AS arr_tail
			, count(DISTINCT airline) AS arr_nunique_airlines
	FROM {{ref("prep_flights")}}
	GROUP BY dest
), airport_tot_stats AS (
				 SELECT faa
						, nunique_from
						, nunique_to
						, dep_planned_tot + arr_planned_tot AS planned_tot
						, dep_cancelled_tot + arr_cancelled_tot AS cancelled_tot
						, dep_diverted_tot + arr_diverted_tot AS diverted_tot
						, dep_occ_tot + arr_occ_tot AS occ_tot
						, dep_tail + arr_tail AS tail_tot
						, dep_nunique_airlines + arr_nunique_airlines AS nunique_airlines_tot
						FROM departures
						JOIN arrivals
						USING (faa)
) 
SELECT a.country
		, a.city
		, a.name 
		, ats.*
FROM airport_tot_stats ats
LEFT JOIN {{ref("prep_airports")}} a
USING (faa)