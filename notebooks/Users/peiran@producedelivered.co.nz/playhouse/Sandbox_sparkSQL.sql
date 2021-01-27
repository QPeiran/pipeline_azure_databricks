-- Databricks notebook source
-- MAGIC %md
-- MAGIC A Databricks database is a collection of tables.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS sandbox;
USE sandbox

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE people

-- COMMAND ----------

SELECT firstName, middleName, lastName, birthdate
FROM people
WHERE year(birthdate) > 1960 AND gender = 'F'

-- COMMAND ----------

select cast(birthDate as timestamp) from people

-- COMMAND ----------

SELECT year(birthDate) as birthYear,  firstName, count (*) AS total
FROM people
WHERE (firstName = 'Aletha' OR firstName = 'Laila') AND gender = 'F'  
  AND year(birthDate) > 1960
GROUP BY birthYear, firstName
ORDER BY birthYear, firstName

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW WomenBornAfter1990 AS
  SELECT firstName, middleName, lastName, year(birthDate) AS birthYear, salary 
  FROM people
  WHERE year(birthDate) > 1990 AND gender = 'F'

-- COMMAND ----------

SELECT * FROM WomenBornAfter1990

-- COMMAND ----------

SELECT ROUND(AVG(salary)) AS averageSalary
FROM people

-- COMMAND ----------

SELECT * from ssa_names

-- COMMAND ----------

SELECT *
FROM ssa_names as ssa
INNER JOIN people as ppl
ON ssa.firstName = ppl.firstName
order by ssa.firstName

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW SSADistinctNames AS 
  SELECT DISTINCT firstName AS ssaFirstName 
  FROM ssa_names;

CREATE OR REPLACE TEMPORARY VIEW PeopleDistinctNames AS 
  SELECT DISTINCT firstName 
  FROM people;
  
SELECT count(firstName) 
FROM PeopleDistinctNames 
INNER JOIN SSADistinctNames ON firstName = ssaFirstName

-- COMMAND ----------

SELECT COUNT(firstName)
FROM PeopleDistinctNames
WHERE firstName IN (
  SELECT ssaFirstName FROM SSADistinctNames
)

-- COMMAND ----------

SELECT * FROM databricks_blog LIMIT 3

-- COMMAND ----------

SELECT title, 
       CAST(dates.publishedOn AS timestamp) AS publishedOn 
FROM databricks_blog

-- COMMAND ----------

show tables

-- COMMAND ----------

SELECT authors, dates, categories,
TRANSFORM (categories, whatever -> LOWER(whatever)) AS lwr_categories
FROM databricks_blog

-- |||                            |||
-- VVV          results           VVV

-- COMMAND ----------

SELECT authors, dates.tz, LOWER(dates.tz) AS lower_tz
FROM databricks_blog

-- COMMAND ----------

