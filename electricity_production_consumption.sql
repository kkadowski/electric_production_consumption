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
	Electricity productio	n from renewable sources, excluding hydroelectric (% of total)	EG.ELC.RNWX.ZS
*/

--Countries
SELECT * 
FROM country c;

--Countries & Region
SELECT c.shortname, 
	c.countrycode, 
	c.region,
	c.alpha2code 
FROM country c
ORDER BY c.region;


-- World / Europe / Asia / another groups of countries - have a number in alpha2code code or letters: 
-- 7E 1W S4 F1 Z7 S3 B8 8S S2 S1 1A Z4 4E XO
-- XU XE XD XR XS XP XQ XT XN XM  XL XJ XC
-- ZJ  EU ZG  ZF ZQ OE 
/*
 *Latin America & Caribbean (all income levels)	ZJ
Europe & Central Asia (developing only)	7E
European Union	EU
World	1W
Other small states	S4
Fragile and conflict affected situations	F1
OECD members	OE
North America	XU
Sub-Saharan Africa (all income levels)	ZG
Europe & Central Asia (all income levels)	Z7
Heavily indebted poor countries (HIPC)	XE
High income	XD
High income: nonOECD	XR
High income: OECD	XS
Sub-Saharan Africa (developing only)	ZF
Caribbean small states	S3
Central Europe and the Baltics	B8
South Asia	8S
Middle income	XP
Middle East & North Africa (developing only)	XQ
Upper middle income	XT
Middle East & North Africa (all income levels)	ZQ
Small states	S1
Arab World	1A
Lower middle income	XN
Low income	XM
East Asia & Pacific (all income levels)	Z4
East Asia & Pacific (developing only)	4E
Low & middle income	XO
Least developed countries: UN classification	XL
Pacific island small states	S2
Latin America & Caribbean (developing only)	XJ
Euro area	XC
 */

--Regions
SELECT * 
FROM country c, 
regexp_matches(alpha2code, '[0-9]');

--List of countries without stats of groups of countries
SELECT * 
FROM country c
WHERE c.alpha2code !~ '[%0-9%]' 
	AND c.alpha2code !~'[X%]' 
	AND c.alpha2code NOT IN ('EU', 'ZJ', 'ZQ', 'OE', 'ZG', 'ZF');
-- OR
SELECT * 
FROM country c
WHERE c.region <>'';

	
--Min and Max year
SELECT min(i."Year")
FROM indicators i; --1960

SELECT max(i."Year") 
FROM indicators i; --2013

-- Countries population to get total consumption / production
DROP TABLE IF EXISTS population;
CREATE TEMP TABLE population
AS
	SELECT countryname, 
		countrycode,
		indicatorcode, 
		indicatorname, 
		"Year", 
		value as population 
	FROM indicators i
	WHERE lower(indicatorname) LIKE '%opulation, tota%'
	ORDER BY countryname, "Year" ;

SELECT * 
FROM population;

--======================================================================
-- Statistics of electr. consumption (per capita) from records 
-- for regions: World / Europe / Asia / etc. 
--======================================================================


-- Electr. consumption (per capita) in regions in years
DROP TABLE IF EXISTS region_electr_consumption_pc;
CREATE TEMP TABLE region_electr_consumption_pc
AS
	SELECT c.shortname AS Region, 
		i."Year",
		round(i.value::NUMERIC, 2) AS consumption
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' AND c.region =''
	GROUP BY c.shortname, i."Year", i.value
	ORDER BY 2;

SELECT * 
FROM region_electr_consumption_pc;

--Average electr. consumption (per capita) by regions 
SELECT Region,
	ROUND(avg(consumption)::NUMERIC,1) avg_consumption
FROM region_electr_consumption_pc
GROUP BY Region
ORDER BY 2 DESC;


--Average electr. consumption in coutries (per capita) by every 10 years 
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
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' AND c.region<>'';
SELECT * 
FROM ten_years;


--Electr. consumption (per capita) by countries
DROP TABLE IF EXISTS consumption_by_countires_pc;
CREATE TEMP TABLE consumption_by_countires_pc
AS
	SELECT c.shortname AS country, 
		i."Year" AS yearof,
		round(i.value::numeric, 1) AS consumption,
		lag(round(i.value::numeric, 1)) OVER (PARTITION BY c.shortname ORDER BY c.shortname, i."Year") consumption_prev
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' AND c.region<>''
	GROUP BY country, yearof, consumption
	ORDER BY 1, 2;
SELECT * 
FROM consumption_by_countires_pc;


-- Percentage increases in consumption (per capita) by countries / years 
DROP TABLE IF EXISTS percent_increases_pc;
CREATE TEMP TABLE percent_increases_pc
AS
	SELECT country, 
		yearof,
		consumption,
		consumption_prev,
		round((consumption - consumption_prev)/ consumption_prev, 3)*100 AS percent_consumption_incr
	FROM consumption_by_countires_pc;
SELECT * 
FROM percent_increases_pc;


-- Country with the greatest increase in consumption  (per capita)
SELECT country,
	yearof,
	percent_consumption_incr
FROM percent_increases_pc
WHERE percent_consumption_incr = (SELECT max(percent_consumption_incr) FROM percent_increases_pc);

-- Country with the largest negative consumption growth  (per capita)
SELECT country,
	yearof,
	percent_consumption_incr
FROM percent_increases_pc
WHERE percent_consumption_incr = (SELECT min(percent_consumption_incr) FROM percent_increases_pc);

	

DROP TABLE IF EXISTS percentyle_pc;
CREATE TEMP TABLE percentyle_pc
AS
	SELECT	yearof,
		percentile_disc(0.95) WITHIN GROUP (ORDER BY percent_consumption_incr) q95,
		percentile_disc(0.5) WITHIN GROUP (ORDER BY percent_consumption_incr) q50,
		percentile_disc(0.05) WITHIN GROUP (ORDER BY percent_consumption_incr) q5
	FROM percent_increases_pc
	GROUP BY 1;
SELECT * 
FROM percentyle_pc;


DROP TABLE IF EXISTS high_pc;
CREATE TEMP TABLE high_pc
AS
	SELECT DISTINCT o.country,
		o.yearof,
		percent_consumption_incr,			
		CASE WHEN percent_consumption_incr >= q50 THEN 1 ELSE 0 END in_q50,
		CASE WHEN percent_consumption_incr >= q95 THEN 1 ELSE 0 END in_q95,
		CASE WHEN percent_consumption_incr <= q5 THEN 1 ELSE 0 END in_q5
	FROM percent_increases_pc o
	CROSS JOIN percentyle_pc;
SELECT * 
FROM high_pc;

--Countries in 95% (per capita)
SELECT o.country,
	   sum(o.in_q95) as sum_q95
FROM high_pc o 
GROUP BY o.country
ORDER BY 2 DESC;

-- Countries in 5% (per capita)
SELECT o.country,
	   sum(in_q5) AS sum_q5
FROM high_pc o 
GROUP BY o.country
ORDER BY 2 DESC;


-- Annual electr. consumption no-group by country (per capita)
DROP TABLE IF EXISTS year_consumption_world_pc;
CREATE TEMP TABLE year_consumption_world_pc
AS
	SELECT i."Year" AS yearof,
		round(i.value::numeric, 1) AS year_consum,
		lag(round(i.value::numeric, 1)) OVER (PARTITION BY  i."Year") year_consum_prev
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' AND c.region<>'' 
	GROUP BY yearof, year_consum
	ORDER BY 1;
select * 
FROM year_consumption_world_pc;


DROP TABLE IF EXISTS avg_year_pc;
CREATE TEMP TABLE avg_year_pc
AS
	SELECT yearof, 
		round(avg(year_consum)::numeric, 2) avg_year_consum,
		round(avg(year_consum_prev)::numeric, 2) avg_year_consum_prev		
	FROM year_consumption_world_pc
	GROUP BY yearof;
SELECT * 
FROM avg_year_pc;

-- The largest increases in average consumption globally (per capita)
SELECT yearof,
		round((avg_year_consum - avg_year_consum_prev)/avg_year_consum_prev,4)*100 as percet_avg_year_consum
FROM avg_year_pc
ORDER BY 2 DESC;

--======================================================================
-- Statistics of electr. consumption (total) from records 
-- for regions: World / Europe / Asia / etc. 
--======================================================================


-- Electr. consumption (total) in regions in years
DROP TABLE IF EXISTS region_electr_consumption_tot;
CREATE TEMP TABLE region_electr_consumption_tot
AS
	SELECT c.shortname AS Region, 
		i."Year",
		round(i.value::NUMERIC, 2) AS percapita,
		p.population AS population,
		round(i.value::NUMERIC, 2) * p.population AS consumption_tot
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	JOIN population p ON i.countrycode =p.countrycode 
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' AND c.region=''
	AND i."Year" = p."Year"
	GROUP BY c.shortname, 
		i."Year", 
		i.value, 
		p.population
	ORDER BY 5 DESC;

SELECT * 
FROM region_electr_consumption_tot;

--Average electr. consumption (total) by regions 
SELECT Region,
	ROUND(avg(consumption_tot)::NUMERIC,1) avg_consumption_tot
FROM region_electr_consumption_tot
GROUP BY Region
ORDER BY 2 DESC;


--Electr. consumption (total) by countries
DROP TABLE IF EXISTS consumption_by_countires_tot;
CREATE TEMP TABLE consumption_by_countires_tot
AS
	SELECT c.shortname AS country, 
		i."Year" AS yearof,
		round((round(i.value::numeric, 1)*p.population)::numeric,1) AS consumption_tot
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	JOIN population p ON c.countrycode =p.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' 
		AND p."Year" = i."Year" AND c.region<>''
	GROUP BY country, yearof, i.value, consumption_tot, p.population
	ORDER BY 1, 2;
SELECT * 
FROM consumption_by_countires_tot;

DROP TABLE IF EXISTS incr_consump_tot;
CREATE TEMP TABLE incr_consump_tot
	AS
	SELECT country,
		yearof,
		consumption_tot,
		lag(consumption_tot) OVER (PARTITION BY country) consumption_prev_tot
	FROM consumption_by_countires_tot
	ORDER BY 1,2;
SELECT * 
FROM incr_consump_tot;

-- Percentage increases in consumption (total) by countries / years 
DROP TABLE IF EXISTS percent_increases_tot;
CREATE TEMP TABLE percent_increases_tot
AS
	SELECT country, 
		yearof,
		consumption_tot,
		consumption_prev_tot,
		round((consumption_tot - consumption_prev_tot)/ consumption_prev_tot, 3)*100 AS percent_consumption_incr_tot
	FROM incr_consump_tot;
SELECT * 
FROM percent_increases_tot;


-- Country with the greatest increase in consumption  (total)
SELECT country,
	yearof,
	percent_consumption_incr_tot
FROM percent_increases_tot
WHERE percent_consumption_incr_tot = (SELECT max(percent_consumption_incr_tot) FROM percent_increases_tot);

-- Country with the largest negative consumption growth  (total)
SELECT country,
	yearof,
	percent_consumption_incr_tot
FROM percent_increases_tot
WHERE percent_consumption_incr_tot = (SELECT min(percent_consumption_incr_tot) FROM percent_increases_tot);

	

DROP TABLE IF EXISTS percentyle_tot;
CREATE TEMP TABLE percentyle_tot
AS
	SELECT	yearof,
		percentile_disc(0.95) WITHIN GROUP (ORDER BY percent_consumption_incr_tot) q95,
		percentile_disc(0.5) WITHIN GROUP (ORDER BY percent_consumption_incr_tot) q50,
		percentile_disc(0.05) WITHIN GROUP (ORDER BY percent_consumption_incr_tot) q5
	FROM percent_increases_tot
	GROUP BY 1;
SELECT * 
FROM percentyle_tot;


DROP TABLE IF EXISTS high_tot;
CREATE TEMP TABLE high_tot
AS
	SELECT DISTINCT o.country,
		o.yearof,
		percent_consumption_incr_tot,			
		CASE WHEN percent_consumption_incr_tot >= q50 THEN 1 ELSE 0 END in_q50,
		CASE WHEN percent_consumption_incr_tot >= q95 THEN 1 ELSE 0 END in_q95,
		CASE WHEN percent_consumption_incr_tot <= q5 THEN 1 ELSE 0 END in_q5
	FROM percent_increases_tot o
	CROSS JOIN percentyle_tot;
SELECT * 
FROM high_tot;

--Countries in 95% (total)
SELECT o.country,
	   sum(o.in_q95) as sum_q95
FROM high_tot o 
GROUP BY o.country
ORDER BY 2 DESC;

-- Countries in 5% (total)
SELECT o.country,
	   sum(in_q5) AS sum_q5
FROM high_tot o 
GROUP BY o.country
ORDER BY 2 DESC;


-- Annual electr. consumption no-group by country (tot)
DROP TABLE IF EXISTS year_consumption_world_tot;
CREATE TEMP TABLE year_consumption_world_tot
AS
	SELECT i.countryname, i."Year" AS yearof,
		round((round(i.value::numeric, 1)*p.population)::numeric, 1) AS year_consum_tot
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	JOIN population p ON c.countrycode =p.countrycode
	WHERE lower(i.indicatorname) LIKE '%electric power cons%' 
		AND p."Year" = i."Year" AND c.region<>''
	GROUP BY i.countryname, yearof, year_consum_tot
	ORDER BY 1;
SELECT * 
FROM year_consumption_world_tot;

DROP TABLE IF EXISTS year_consumption_world_tot_lag;
CREATE TEMP TABLE year_consumption_world_tot_lag
AS
SELECT yearof,
	year_consum_tot,
	lag(year_consum_tot) OVER (PARTITION BY yearof) as year_consum_prev_tot
FROM year_consumption_world_tot;
SELECT *
FROM year_consumption_world_tot_lag;


DROP TABLE IF EXISTS avg_year_tot;
CREATE TEMP TABLE avg_year_tot 
AS
	SELECT yearof, 
		round(avg(year_consum_tot)::numeric, 2) avg_year_consum_tot,
		round(avg(year_consum_prev_tot)::numeric, 2) avg_year_consum_prev_tot		
	FROM year_consumption_world_tot_lag
	GROUP BY yearof;
SELECT * 
FROM avg_year_tot;

-- The largest increases in average consumption globally (total)
SELECT yearof,
		round((avg_year_consum_tot - avg_year_consum_prev_tot)/avg_year_consum_prev_tot,4)*100 as percet_avg_year_consum_tot
FROM avg_year_tot
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
	i.indicatorname  AS indicator_name,
	i.indicatorcode AS icode,
	avg(round(i.value::numeric, 1)) AS production 
FROM indicators i
JOIN country c ON i.countrycode = c.countrycode
JOIN population p ON c.countrycode = p.countrycode
WHERE lower(i.indicatorname) LIKE '%electricity prod%' AND p."Year" = i."Year" AND c.region<>'' AND lower(i.indicatorcode) LIKE '%zs'
GROUP BY  i."Year" , c.shortname, i.indicatorname, i.indicatorcode 
ORDER BY (1,2); 

-- Extention for crosstab
CREATE extension tablefunc;

-- Production in countries (without Regions)
DROP TABLE IF EXISTS prod_temp;
CREATE TEMP TABLE prod_temp
AS
SELECT c.shortname AS country, 
	i.indicatorname  AS indicator_name, 
	i.indicatorcode As icode,
	round(i.value::numeric, 1) production
FROM indicators i 
JOIN country c ON i.countrycode = c.countrycode
JOIN population p ON c.countrycode = p.countrycode
WHERE lower(i.indicatorname) LIKE'%electricity prod%' AND lower(i.indicatorcode) LIKE'%zs' AND i.value <>0 AND p."Year" = i."Year" AND c.region<>''
GROUP BY c.shortname, i.indicatorname, i.indicatorcode,i.value
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
		avg(production) as avg_prod 
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
	JOIN population p ON c.countrycode = p.countrycode
	WHERE lower(i.indicatorname) LIKE '%electricity prod%' AND lower(i.indicatorcode) LIKE '%zs' AND i.value <>0 AND p."Year" = i."Year" AND c.region<>''
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

--
SELECT yearof,
	round((avg_year_produc - avg_year_produc_prev)/avg_year_produc_prev,4)*100 AS percent_avg_year_produc_incr
FROM avg_produc
ORDER BY 2 ;

-- ====================================================
-- Production in Regions

DROP TABLE IF EXISTS prod_temp_regions;
CREATE TEMP TABLE prod_temp_regions
AS
SELECT c.shortname AS country, 
	i.indicatorname  AS indicator_name, 
	i.indicatorcode As icode,
	round(i.value::numeric, 1) production
FROM indicators i 
JOIN country c ON i.countrycode = c.countrycode
JOIN population p ON c.countrycode = p.countrycode
WHERE lower(i.indicatorname) LIKE'%electricity prod%' AND lower(i.indicatorcode) LIKE'%zs' AND i.value <>0 AND p."Year" = i."Year" AND c.region=''
GROUP BY c.shortname, i.indicatorname, i.indicatorcode, i.value 
ORDER BY (1,2); 

SELECT country, 
	icode, 
	production
FROM prod_temp_regions
WHERE icode LIKE '%ZS'
ORDER BY 1,2;

-- Electricity production in % / kWh of totalby countries
DROP TABLE IF EXISTS cross_production_regions;
CREATE TEMP TABLE cross_production_regions
AS
	SELECT * 
	FROM crosstab('
		select country,
		icode, 
		avg(production) as avg_prod 
		from prod_temp_regions
		group by country, icode
		order by 1,2 ')
	AS final_result_regions(
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
FROM cross_production_regions;
	
-- Highest production from COAL (% of total)
SELECT country, "EG.ELC.COAL.ZS"
FROM cross_production_regions
WHERE "EG.ELC.COAL.ZS" IS NOT NULL
ORDER by 2 DESC;

-- Lowest / no production from COAL (% of total)
SELECT country, "EG.ELC.COAL.ZS"
FROM cross_production_regions
ORDER by 2;

-- Highest production from oil, gas and coal (% of total)
SELECT country, "EG.ELC.FOSL.ZS"
FROM cross_production_regions
WHERE "EG.ELC.FOSL.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from oil, gas and coal (% of total)
SELECT country, "EG.ELC.FOSL.ZS"
FROM cross_production_regions
ORDER BY 2;

-- Highest production from hydroelectric sources (% of total)
SELECT country, "EG.ELC.HYRO.ZS"
FROM cross_production_regions
WHERE "EG.ELC.HYRO.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from hydroelectric sources (% of total)
SELECT country, "EG.ELC.HYRO.ZS"
FROM cross_production_regions
ORDER BY 2;

-- Highest production from natural gas sources (% of total)
SELECT country, "EG.ELC.NGAS.ZS"
FROM cross_production_regions
WHERE "EG.ELC.NGAS.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from natural gas sources (% of total)
SELECT country, "EG.ELC.NGAS.ZS"
FROM cross_production_regions
ORDER BY 2;

-- Highest production from nuclear sources (% of total)
SELECT country, "EG.ELC.NUCL.ZS"
FROM cross_production_regions
WHERE "EG.ELC.NUCL.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from nuclear sources (% of total)
SELECT country, "EG.ELC.NUCL.ZS"
FROM cross_production_regions
ORDER BY 2;

-- Highest production from oil sources (% of total)
SELECT country, "EG.ELC.PETR.ZS"
FROM cross_production_regions
WHERE "EG.ELC.PETR.ZS" IS NOT NULL
ORDER BY 2 DESC;

-- Lowest / no production from oil sources (% of total)
SELECT country, "EG.ELC.PETR.ZS"
FROM cross_production_regions
ORDER BY 2;

-- Highest production from renewable sources, excluding hydroelectric (% of total)
SELECT country, "EG.ELC.RNWX.ZS"
FROM cross_production_regions
WHERE "EG.ELC.RNWX.ZS" IS NOT NULL
ORDER by 2 DESC;

-- Lowest / no production from renewable sources, excluding hydroelectric (% of total)
SELECT country, "EG.ELC.RNWX.ZS"
FROM cross_production_regions
ORDER by 2;


-- Annual production without grouping by countries 
DROP TABLE IF EXISTS year_produc_world_regions;
CREATE TEMP TABLE year_produc_world_regions
AS
	SELECT i."Year" AS yearof,
		round(i.value::numeric, 1) AS year_produc,
		lag(round(i.value::numeric, 1)) OVER (PARTITION BY  i."Year") year_produc_prev
	FROM indicators i
	JOIN country c ON i.countrycode = c.countrycode
	JOIN population p ON c.countrycode = p.countrycode
	WHERE lower(i.indicatorname) LIKE '%electricity prod%' AND lower(i.indicatorcode) LIKE '%zs' AND i.value <>0 AND p."Year" = i."Year" AND c.region=''
	GROUP BY yearof, year_produc
	ORDER BY 1;
SELECT *
FROM year_produc_world_regions;

DROP TABLE IF EXISTS avg_produc_regions;
CREATE TEMP TABLE avg_produc_regions
AS
	SELECT yearof, 
		round(avg(year_produc)::numeric, 2) avg_year_produc,
		round(avg(year_produc_prev)::numeric, 2) avg_year_produc_prev		
	FROM year_produc_world_regions
	GROUP BY yearof;
SELECT * 
FROM avg_produc_regions;

--
SELECT yearof,
	round((avg_year_produc - avg_year_produc_prev)/avg_year_produc_prev,4)*100 AS percent_avg_year_produc_incr
FROM avg_produc_regions
ORDER BY 2 DESC;


-- to be continued...





