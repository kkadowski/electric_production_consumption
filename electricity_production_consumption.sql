/*

WORLD DEVELOPMENT INDICATORS
Electricity production / consumption (1960 - 2012)

Data from kaggle.com
Krzysztof Kadowski, 2021-06-10

**/
--=============================================================================
--Data preparation
--=============================================================================

--All indocators for electricity
SELECT* FROM indicators i
WHERE lower(indicatorname) LIKE '%electr%'
ORDER BY indicatorname;

--Indicators for production
SELECT DISTINCT indicatorname, indicatorcode 
FROM indicators i
WHERE lower(indicatorname) LIKE '%electricity prod%'
ORDER BY indicatorcode;

--Indicators for consumption
SELECT DISTINCT indicatorname, indicatorcode 
FROM indicators i
WHERE lower(indicatorname) LIKE '%electric power cons%'
ORDER BY indicatorcode;

/*
Indicators + codes: 
	Electric power consumption (kWh per capita)	EG.USE.ELEC.KH.PC
	
	Electricity production from coal sources (% of total)	EG.ELC.COAL.ZS
	Electricity production from oil, gas and coal sources (% of total)	EG.ELC.FOSL.ZS
	Electricity production from hydroelectric sources (% of total)	EG.ELC.HYRO.ZS
	Electricity production from natural gas sources (% of total)	EG.ELC.NGAS.ZS
	Electricity production from nuclear sources (% of total)	EG.ELC.NUCL.ZS
	Electricity production from oil sources (% of total)	EG.ELC.PETR.ZS
	Electricity production from renewable sources, excluding hydroelectric (kWh)	EG.ELC.RNWX.KH
	Electricity production from renewable sources, excluding hydroelectric (% of total)	EG.ELC.RNWX.ZS
*/

--Countries
SELECT * 
FROM country c;

-- World / Europe / Asia / another groups of countries - have a number in alpha2code code or letters: 
-- XC, EU, XE, XD, XR, XS, XJ, ZJ, XL XO, XM, XN, ZQ, XQ, XP, XU, OE,  ZG, ZF, XT

--Regions
SELECT * 
FROM country c, 
regexp_matches(alpha2code, '[0-9]');

--List of countries without stats of groups of countries
SELECT * FROM country c
WHERE c.alpha2code !~ '[%0-9%]' 
	AND c.alpha2code !~'[X%]' 
	AND c.alpha2code NOT IN ('EU', 'ZJ', 'ZQ', 'OE', 'ZG', 'ZF');
	
--Min and Max year
SELECT min(i."Year")
FROM indicators i; --1960

SELECT max(i."Year") 
FROM indicators i; --2013

--======================================================================
-- Statistics of electr. consumption per capita from records 
-- for regions: World / Europe / Asia / etc. 
--======================================================================

-- Electr. consumption in regions in years
DROP TABLE IF EXISTS region_electr_consumption;
CREATE TEMP TABLE region_electr_consumption
AS
	SELECT c.shortname AS Region, 
		i."Year",
		round(i.value::NUMERIC, 2) AS consumption,
		regexp_matches(alpha2code, '[0-9]')
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%'
	GROUP BY c.shortname, i."Year", i.value, regexp_matches(alpha2code, '[0-9]')
	ORDER BY 2;

SELECT * 
FROM region_electr_consumption;


--Average electr. consumption by regions 
SELECT Region,
	ROUND(avg(consumption)::NUMERIC,1) avg_consumption
FROM region_electr_consumption
GROUP BY Region
ORDER BY 2;


--Average electr. consumption by every 10 years 
DROP TABLE IF EXISTS ten_years;
CREATE TEMP TABLE ten_years
AS
	SELECT 	avg(i.value) filter (where i."Year" <1970) AS to_1970,
		avg(i.value) filter (where i."Year">=1970 AND i."Year" <1980) AS to_1980,
		avg(i.value) filter (where i."Year">=1980 AND i."Year" <1990) AS to_1990,
		avg(i.value) filter (where i."Year">=1990 AND i."Year" <2000) AS to_2000,
		avg(i.value) filter (where i."Year">=2000 AND i."Year" <2010) AS to_2010,
		avg(i.value) filter (where i."Year">=2010 AND i."Year" <2013) AS to_2013
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' 
		AND c.alpha2code !~ '[%0-9%]' 
		AND c.alpha2code !~'[X%]' 
		AND c.alpha2code NOT IN ('EU', 'ZJ', 'ZQ', 'OE', 'ZG', 'ZF');
SELECT * FROM ten_years;


--Electr. consumption by countries
DROP TABLE IF EXISTS consumption_by_countires;
CREATE TEMP TABLE consumption_by_countires
AS
	SELECT c.shortname AS country, 
		i."Year" AS yearof,
		round(i.value::numeric, 1) AS consumption,
		lag(round(i.value::numeric, 1)) OVER (PARTITION BY c.shortname ORDER BY c.shortname, i."Year") consumption_prev
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' 
		AND c.alpha2code !~ '[%0-9%]' 
		AND c.alpha2code !~'[X%]' 
		AND c.alpha2code NOT IN ('EU', 'ZJ', 'ZQ', 'OE', 'ZG', 'ZF') 
	GROUP BY country, yearof, consumption
	ORDER BY 1, 2;
SELECT * FROM consumption_by_countires;


-- Percentage increases in consumption by countries / years 
DROP TABLE IF EXISTS percent_increases;
CREATE TEMP TABLE percent_increases
AS
	SELECT country, 
		yearof,
		consumption,
		consumption_prev,
		round((consumption - consumption_prev)/ consumption_prev, 3)*100 AS percent_consumption_incr
	FROM consumption_by_countires;
SELECT * 
FROM percent_increases;


-- Country with the greatest increase in consumption 
SELECT country,
	yearof,
	percent_consumption_incr
FROM percent_increases
WHERE percent_consumption_incr = (SELECT max(percent_consumption_incr) FROM percent_increases);

-- Country with the largest negative consumption growth 
SELECT country,
	yearof,
	percent_consumption_incr
FROM percent_increases
WHERE percent_consumption_incr = (SELECT min(percent_consumption_incr) FROM percent_increases);

	

DROP TABLE IF EXISTS percentyle;
CREATE TEMP TABLE percentyle
AS
	SELECT	yearof,
		percentile_disc(0.95) WITHIN GROUP (ORDER BY percent_consumption_incr) q95,
		percentile_disc(0.5) WITHIN GROUP (ORDER BY percent_consumption_incr) q50,
		percentile_disc(0.05) WITHIN GROUP (ORDER BY percent_consumption_incr) q5
	FROM percent_increases
	GROUP BY 1;
SELECT * 
FROM percentyle;


DROP TABLE IF EXISTS high;
CREATE TEMP TABLE high
AS
	SELECT DISTINCT o.country,
		o.yearof,
		percent_consumption_incr,			
		CASE WHEN percent_consumption_incr >= q50 THEN 1 ELSE 0 END in_q50,
		CASE WHEN percent_consumption_incr >= q95 THEN 1 ELSE 0 END in_q95,
		CASE WHEN percent_consumption_incr <= q5 THEN 1 ELSE 0 END in_q5
	FROM percent_increases o
	CROSS JOIN percentyle;
SELECT * 
FROM high;

--Countries in 95%
SELECT o.country,
	   sum(o.in_q95) as sum_q95
FROM high o 
GROUP BY o.country
ORDER BY 2 DESC;

-- Countries in 5%
SELECT o.country,
	   sum(in_q5) AS sum_q5
FROM high o 
GROUP BY o.country
ORDER BY 2 DESC;


-- Annual electr. consumption no-group by country 
DROP TABLE IF EXISTS year_consumption_world;
CREATE TEMP TABLE year_consumption_world
AS
	SELECT i."Year" AS yearof,
		round(i.value::numeric, 1) AS year_consum,
		lag(round(i.value::numeric, 1)) OVER (PARTITION BY  i."Year") year_consum_prev
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' 
		AND c.alpha2code !~ '[%0-9%]' 
		AND c.alpha2code !~'[X%]' 
		AND c.alpha2code NOT IN ('EU', 'ZJ', 'ZQ', 'OE', 'ZG', 'ZF') 
	GROUP BY yearof, year_consum
	ORDER BY 1;
select * from year_consumption_world;


DROP TABLE IF EXISTS avg_year;
CREATE TEMP TABLE avg_year 
AS
	SELECT yearof, 
		round(avg(year_consum)::numeric, 2) avg_year_consum,
		round(avg(year_consum_prev)::numeric, 2) avg_year_consum_prev		
	FROM year_consumption_world
	GROUP BY yearof;
SELECT * 
FROM avg_year;

-- The largest increases in average consumption globally 
SELECT yearof,
		round((avg_year_consum - avg_year_consum_prev)/avg_year_consum_prev,4)*100 as percet_avg_year_consum
FROM avg_year
ORDER BY 2 DESC;



--============================================================
--Electricity production in % / kWh of total
--============================================================

-- Indicators of electr. production
SELECT DISTINCT indicatorname, 
	indicatorcode 
FROM indicators i
WHERE lower(indicatorname) LIKE '%electricity prod%'
ORDER BY indicatorcode;

SELECT i."Year" AS yearof, 
	c.shortname AS country, 
	i.indicatorname  Asindicator_name,
	i.indicatorcode AS icode,
	sum(round(i.value::numeric, 1)) AS production 
FROM indicators i
JOIN country c ON i.countrycode = c.countrycode
WHERE lower(i.indicatorname) LIKE '%electricity prod%'
GROUP BY  i."Year" , c.shortname, i.indicatorname, i.indicatorcode 
ORDER BY (1,2); 

-- Extention for crosstab
CREATE extension tablefunc;


DROP TABLE IF EXISTS prod_temp;
CREATE TEMP TABLE prod_temp
AS
SELECT c.shortname AS country, 
	i.indicatorname  AS indicator_name, 
	i.indicatorcode As icode,
	sum(round(i.value::numeric, 1)) production
FROM indicators i 
JOIN country c ON i.countrycode = c.countrycode
WHERE lower(i.indicatorname) LIKE'%electricity prod%' AND lower(i.indicatorcode) LIKE'%zs' AND i.value <>0
GROUP BY c.shortname, i.indicatorname, i.indicatorcode 
ORDER BY (1,2); 

SELECT country, 
	icode, 
	production
FROM prod_temp 
WHERE icode LIKE '%ZS'
ORDER BY 1,2;

-- Electricity production in % / kWh of totalby countries
DROP TABLE IF EXISTS cross_production;
CREATE TEMP TABLE cross_production
AS
	SELECT * 
	FROM crosstab('
		select country,
		icode, 
		sum(production) as sum_prod 
		from prod_temp 
		group by country, icode
		order by 1,2 ')
	AS final_result(
		country varchar(200),
		"EG.ELC.COAL.ZS" numeric,
		"EG.ELC.FOSL.ZS" numeric,
		"EG.ELC.HYRO.ZS" numeric,
		"EG.ELC.NGAS.ZS" numeric,
		"EG.ELC.NUCL.ZS" numeric,
		"EG.ELC.PETR.ZS" numeric,
		"EG.ELC.RNWX.ZS" numeric)
	ORDER BY 2 DESC;
SELECT *
FROM cross_production;
	
-- Highest production from COAL (% of total)
SELECT country, "EG.ELC.COAL.ZS"
FROM cross_production
WHERE "EG.ELC.COAL.ZS" IS NOT NULL
ORDER by 2 DESC;

-- Lowest / no production from COAL (% of total)
SELECT country, "EG.ELC.COAL.ZS"
FROM cross_production
ORDER by 2;

-- Highest production from oil, gas and coal (% of total)
SELECT country, "EG.ELC.FOSL.ZS"
FROM cross_production
WHERE "EG.ELC.FOSL.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from oil, gas and coal (% of total)
SELECT country, "EG.ELC.FOSL.ZS"
FROM cross_production
ORDER BY 2;

-- Highest production from hydroelectric sources (% of total)
SELECT country, "EG.ELC.HYRO.ZS"
FROM cross_production
WHERE "EG.ELC.HYRO.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from hydroelectric sources (% of total)
SELECT country, "EG.ELC.HYRO.ZS"
FROM cross_production
ORDER BY 2;

-- Highest production from natural gas sources (% of total)
SELECT country, "EG.ELC.NGAS.ZS"
FROM cross_production
WHERE "EG.ELC.NGAS.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from natural gas sources (% of total)
SELECT country, "EG.ELC.NGAS.ZS"
FROM cross_production
ORDER BY 2;

-- Highest production from nuclear sources (% of total)
SELECT country, "EG.ELC.NUCL.ZS"
FROM cross_production
WHERE "EG.ELC.NUCL.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from nuclear sources (% of total)
SELECT country, "EG.ELC.NUCL.ZS"
FROM cross_production
ORDER BY 2;

-- Highest production from oil sources (% of total)
SELECT country, "EG.ELC.PETR.ZS"
FROM cross_production
WHERE "EG.ELC.PETR.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from oil sources (% of total)
SELECT country, "EG.ELC.PETR.ZS"
FROM cross_production
ORDER BY 2;

-- Highest production from renewable sources, excluding hydroelectric (% of total)
SELECT country, "EG.ELC.RNWX.ZS"
FROM cross_production
WHERE "EG.ELC.RNWX.ZS" IS NOT NULL
ORDER by 2 DESC;

-- Lowest / no production from renewable sources, excluding hydroelectric (% of total)
SELECT country, "EG.ELC.RNWX.ZS"
FROM cross_production
ORDER by 2;


-- Annual production without grouping by countries 
DROP TABLE IF EXISTS year_produc_world;
CREATE TEMP TABLE year_produc_world
AS
	SELECT i."Year" AS yearof,
		round(i.value::numeric, 1) AS year_produc,
		lag(round(i.value::numeric, 1)) OVER (PARTITION BY  i."Year") year_produc_prev
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	WHERE lower(i.indicatorname) LIKE '%electricity prod%' AND lower(i.indicatorcode) LIKE '%zs' AND i.value <>0
	GROUP BY yearof, year_produc
	ORDER BY 1;
SELECT *
FROM year_produc_world;

DROP TABLE IF EXISTS avg_produc;
CREATE TEMP TABLE avg_produc
AS
	SELECT yearof, 
		round(avg(year_produc)::numeric, 2) avg_year_produc,
		round(avg(year_produc_prev)::numeric, 2) avg_year_produc_prev		
	FROM year_produc_world
	GROUP BY yearof;
SELECT * 
FROM avg_produc;

SELECT yearof,
	round((avg_year_produc - avg_year_produc_prev)/avg_year_produc_prev,4)*100 AS percent_avg_year_produc_incr
FROM avg_produc
ORDER BY 2 DESC;


-- to be continued...
