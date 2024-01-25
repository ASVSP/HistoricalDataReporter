-- 1. Najvise zagadjen mesec 2015. godine.
with QuantityPerMonth as (
    select sum(ge.quantity) as total_gas_emission, ge.month_id as month_id,
		m.name as month_name 
	from gas_emission ge inner join month m
	on ge.month_id=m.id
	inner join year y
	on m.year_id=y.id
	where y.value=2015
	group by ge.month_id, m.name
)
    select qpm.total_gas_emission as max_total_emission, qpm.month_id, qpm.month_name
    from QuantityPerMonth qpm
    order by max_total_emission desc
    limit 1;

-- 2. Izracunaj kumulativnu vrednost emisije gasa za svaku drzavu, uzimajuci
-- u obzir samo top 2 tipa gasa za svaku godinu
WITH TopGasTypes AS (
  SELECT y.value AS year, co.code AS country_code, g.name AS gas_name,
         RANK() OVER (PARTITION BY y.value, co.code ORDER BY SUM(ce.quantity) DESC) AS gas_rank
  FROM year y
  JOIN month m ON y.id = m.year_id
  JOIN gas_emission ce ON m.id = ce.month_id
  JOIN country co ON ce.country_code = co.code
  JOIN gas g ON ce.gas_id = g.id
  GROUP BY y.value, co.code, g.name
)
    SELECT co.name AS country_name, t.year AS year,
           round(SUM(ce.quantity), 2) AS cumulative_emission
    FROM TopGasTypes t, year y, gas g
    JOIN gas_emission ce ON t.year = y.value AND t.country_code = ce.country_code AND t.gas_name = g.name
    JOIN country co ON ce.country_code = co.code
    WHERE t.gas_rank <= 2
    GROUP BY co.name, t.year;

-- 3. Pronadji godinu kada je bio najveci porast zagadjenosti izmedju dve uzastopne godine, za svaki kontinent
WITH EmissionChanges AS (
    SELECT c.name AS continent_name,
           y.value AS year,
           SUM(ce.quantity) - LAG(SUM(ce.quantity)) OVER (PARTITION BY c.name ORDER BY y.value) AS quantity_increase
    FROM continent c
    JOIN region r ON c.code = r.continent_code
    JOIN country co ON r.code = co.region_code
    JOIN gas_emission ce ON co.code = ce.country_code
    JOIN month m ON ce.month_id = m.id
    JOIN year y ON m.year_id = y.id
    GROUP BY c.name, y.value
)
, MaxIncreasePerContinent AS (
    SELECT continent_name, MAX(quantity_increase) AS max_increase
    FROM EmissionChanges
    GROUP BY continent_name
)
    SELECT e.continent_name AS continent_name, e.year as year, round(e.quantity_increase, 2) AS max_increase
    FROM EmissionChanges e
    JOIN MaxIncreasePerContinent m ON e.continent_name = m.continent_name AND e.quantity_increase = m.max_increase;

-- 4. Pronaci za svaku drzavu u evropi godinu i informaciju da li se zagadjenost te drzave povecala, smanjila ili ostala
-- otpirilike ista u odnosu na godinu pre
WITH EmissionRank AS (
  SELECT co.name AS country_name, y.value AS year,
         RANK() OVER (PARTITION BY co.code ORDER BY SUM(ce.quantity) DESC) AS emission_rank
  FROM year y
  JOIN month m ON y.id = m.year_id
  JOIN gas_emission ce ON m.id = ce.month_id
  JOIN country co ON ce.country_code = co.code
  JOIN region r ON co.region_code = r.code
  JOIN continent c ON c.code=r.continent_code
  WHERE c.name = 'Europe'
  GROUP BY co.name, co.code, y.value
)
    SELECT er1.country_name, er1.year,
           CASE WHEN er1.emission_rank = er2.emission_rank THEN 'Stable'
                WHEN er1.emission_rank < er2.emission_rank THEN 'Increased'
                ELSE 'Decreased' END AS rank_change
    FROM EmissionRank er1
    JOIN EmissionRank er2 ON er1.country_name = er2.country_name AND er1.year = er2.year - 1;

--select c.name as country, y.value as year, SUM(ge.quantity) as sum
--FROM gas_emission ge
--INNER JOIN month m
--on m.id = ge.month_id
--INNER JOIN country c
--on c.code = ge.country_code
--INNER JOIN year y
--on m.year_id = y.id
--group by c.name, y.value;

-- 5. Rangiraj drzave Azije iz 2020. godine po zagadjenosti u opadajucem redosledu (~25s)
SELECT y.value AS year, co.name AS country_name,
       ROUND(SUM(ce.quantity), 2) AS total_gas_emission,
       RANK() OVER (PARTITION BY y.value ORDER BY SUM(ce.quantity) DESC) AS emission_rank
FROM year y
JOIN month m ON y.id = m.year_id
JOIN gas_emission ce ON m.id = ce.month_id
JOIN country co ON ce.country_code = co.code
JOIN region r ON co.region_code = r.code
JOIN continent c ON c.code = r.continent_code
WHERE c.name = 'Asia' and y.value=2020
GROUP BY y.value, co.name;

-- 6. Pronadji top 3 drzave, za svaki tip gasa, sa najvecom emisijom tog tipa (za svaku godinu) (~1min44s)
WITH GasEmissionRank AS (
    SELECT
        g.name AS gas_name,
        co.name AS country_name,
        y.value AS year,
        SUM(ce.quantity) AS total_gas_emission,
        RANK() OVER (PARTITION BY g.name, y.value ORDER BY SUM(ce.quantity) DESC) AS emission_rank
    FROM
        gas_emission ce
        JOIN country co ON ce.country_code = co.code
        JOIN gas g ON ce.gas_id = g.id
        JOIN month m ON ce.month_id = m.id
        JOIN year y ON m.year_id = y.id
    GROUP BY
        g.name, co.name, y.value
)
    SELECT
        gas_name,
        country_name,
        year,
        round(total_gas_emission, 2) as total_emission
    FROM
        GasEmissionRank
    WHERE
        emission_rank <= 3;

-- 7. Identifikuj drzave kod koje odstupaju po proseku emisije svakog tipa gasa u odnosu na globalni prosek
WITH global_avg_gas_emission AS (
  SELECT ge.gas_id, AVG(ge.quantity) AS avg_gas_emission
  FROM gas_emission ge
  GROUP BY ge.gas_id
)
    SELECT c.name AS country_name, g.name AS gas_name, AVG(ge.quantity) AS avg_country_gas_emission,
           global_avg.avg_gas_emission AS global_avg_gas_emission
    FROM country c
    JOIN gas_emission ge ON c.code = ge.country_code
    JOIN gas g ON ge.gas_id = g.id
    JOIN global_avg_gas_emission global_avg ON g.id = global_avg.gas_id
    GROUP BY c.name, g.name, global_avg.avg_gas_emission
    HAVING AVG(ge.quantity) > 1.5 * global_avg.avg_gas_emission;

-- 8. Drzave u kojima je emitovana najveca kolicina gasa, ispis gasa i kolicine za svaku godinu
WITH ranked_emitters AS (
   SELECT c.name AS country_name, y.value AS year, g.name as gas, ge.quantity as quantity,
          ROW_NUMBER() OVER (PARTITION BY y.value ORDER BY ge.quantity DESC) AS rank
   FROM country c
   JOIN gas_emission ge ON c.code = ge.country_code
   JOIN month m ON ge.month_id = m.id
   JOIN year y ON m.year_id = y.id
   JOIN gas g ON g.id=ge.gas_id
   GROUP BY c.name, y.value, g.name, ge.quantity
)
    SELECT country_name, year, gas, quantity
    FROM ranked_emitters
    WHERE rank = 1
    ORDER BY year;

-- 9. Pregled promene proseka svakog tipa gasa na 3 meseca u Severnoj Americi 2021. godine
SELECT c.name as country, y.value as year, m.num_value as month, g.name as gas,
       AVG(ge.quantity) OVER (PARTITION BY ge.country_code,ge.gas_id ORDER BY y.value, m.num_value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg
FROM gas_emission ge
JOIN month m ON ge.month_id = m.id
JOIN year y ON m.year_id = y.id
JOIN country c ON ge.country_code = c.code
JOIN region r ON c.region_code=r.code
JOIN gas g ON g.id=ge.gas_id
WHERE r.name = 'Northern America' and y.value = 2021
ORDER BY year, month, country, gas;

-- 10. Ispisi u opadajucem redosledu kontinente od onih sa najvecom povecanom razlikom emisije gasa, ka onima sa najmanjom
-- povecanom emisijom
WITH ContinentEmissionsByYear AS (
    SELECT
        co.name AS continent,
        y.value AS year,
        SUM(ge.quantity) AS total_emission
    FROM
        gas_emission ge
        JOIN country c ON ge.country_code = c.code
        JOIN region r ON c.region_code = r.code
        JOIN continent co ON co.code=r.continent_code
        JOIN month m ON ge.month_id = m.id
        JOIN year y ON m.year_id = y.id
    GROUP BY
        co.name, y.value
)
    SELECT
        continent,
        MAX(total_emission) - MIN(total_emission) AS emission_increase
    FROM ContinentEmissionsByYear
    GROUP BY continent
    ORDER BY emission_increase DESC;


