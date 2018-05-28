-- Databricks notebook source
CREATE TABLE GRAPH
USING CSV
OPTIONS (path"/FileStore/tables/graph.csv",header "true" );

-- COMMAND ----------

select * from graph limit 10;


-- COMMAND ----------

CREATE TABLE ONE_DEGREE_TARGET 
USING CSV
OPTIONS(PATH "/FileStore/tables/one_degree_target.csv", HEADER "TRUE");

-- COMMAND ----------

SELECT * FROM ONE_DEGREE_TARGET LIMIT 10;

-- COMMAND ----------

CREATE TABLE PEOPLE
USING CSV
OPTIONS(PATH "/FileStore/tables/people.csv", HEADER "TRUE");

-- COMMAND ----------

SELECT * FROM PEOPLE LIMIT 10;


-- COMMAND ----------

CREATE TABLE RANDOM_TARGETS
USING CSV
OPTIONS(PATH "/FileStore/tables/random_targets.csv", HEADER = "TRUE");

-- COMMAND ----------

SELECT * FROM RANDOM_TARGETS LIMIT 10;


-- COMMAND ----------

CREATE TABLE RECENT_PURCHASES 
USING CSV
OPTIONS(PATH "/FileStore/tables/recent_purchases.csv",HEADER = "TRUE");

-- COMMAND ----------

SELECT * FROM RECENT_PURCHASES LIMIT 10;

-- COMMAND ----------

SELECT COUNT(UID)*100/
       (SELECT COUNT(*)
       FROM ONE_DEGREE_TARGET) AS CLICKED_BOUGHT
FROM ONE_DEGREE_TARGET
WHERE ad_action = "clicked" AND buy_action = "bought_car";

-- COMMAND ----------

SELECT COUNT(uid) 
FROM ONE_DEGREE_TARGET
WHERE ad_action = "clicked" AND buy_action = "bought_car";

-- COMMAND ----------

SELECT COUNT(UID)*100/
       (SELECT COUNT(*)
       FROM ONE_DEGREE_TARGET) AS CLICKED_NOTBOUGHT
FROM ONE_DEGREE_TARGET
WHERE ad_action = "clicked" AND buy_action is null;

-- COMMAND ----------

SELECT COUNT(UID)
FROM ONE_DEGREE_TARGET
WHERE ad_action = "clicked" AND buy_action is null;

-- COMMAND ----------

SELECT COUNT(UID)*100/
       (SELECT COUNT(*)
       FROM ONE_DEGREE_TARGET) AS CLICKED_BOUGHT
FROM ONE_DEGREE_TARGET
WHERE ad_action = "did_not_click" AND buy_action = "bought_car";

-- COMMAND ----------

SELECT COUNT(uid) 
FROM ONE_DEGREE_TARGET
WHERE ad_action = "did_not_click" AND buy_action = "bought_car";

-- COMMAND ----------

SELECT COUNT(UID)*100/
       (SELECT COUNT(*)
       FROM ONE_DEGREE_TARGET) AS NOTCLICKED_NOTBOUGHT
FROM ONE_DEGREE_TARGET
WHERE ad_action = "did_not_click" AND buy_action is null;

-- COMMAND ----------

SELECT COUNT(*)
FROM ONE_DEGREE_TARGET
WHERE ad_action = "did_not_click" AND buy_action is null;

-- COMMAND ----------

SELECT COUNT(UID)*100/
       (SELECT COUNT(*)
       FROM RANDOM_TARGETS) AS NOTCLICKED_BOUGHT
FROM RANDOM_TARGETS
WHERE ad_action = "did_not_click" AND buy_action = "bought_car";

-- COMMAND ----------

SELECT COUNT(uid) 
FROM RANDOM_TARGETS
WHERE ad_action = "did_not_click" AND buy_action = "bought_car";

-- COMMAND ----------

SELECT COUNT(uid) 
FROM RANDOM_TARGETS
WHERE ad_action = "clicked" AND buy_action = "bought_car";

-- COMMAND ----------

SELECT COUNT(uid)*100/
  (SELECT COUNT (*) FROM RANDOM_TARGETS) AS CLICKED_NOTBOUGHT
FROM RANDOM_TARGETS
WHERE ad_action = "clicked" AND buy_action is null;


-- COMMAND ----------

SELECT COUNT(uid) 
FROM RANDOM_TARGETS
WHERE ad_action = "clicked" AND buy_action IS NULL;

-- COMMAND ----------

SELECT count(uid)*100/
  (SELECT count(*) FROM RANDOM_TARGETS) AS NOTCLICKED_NOTBOUGHT
FROM RANDOM_TARGETS
WHERE ad_action = "did_not_click" AND buy_action IS null;

-- COMMAND ----------

SELECT COUNT(uid) 
FROM RANDOM_TARGETS
WHERE ad_action = "did_not_click" AND buy_action IS null;

-- COMMAND ----------

SELECT PEOPLE.Gender, COUNT(*) AS TOTAL 
FROM ONE_DEGREE_TARGET
JOIN PEOPLE
  ON ONE_DEGREE_TARGET.uid = PEOPLE.uid
WHERE ad_action = "clicked" AND buy_action = "bought_car"
GROUP BY GENDER;

-- COMMAND ----------

SELECT PEOPLE.Race, COUNT(*) AS TOTAL
FROM PEOPLE
JOIN ONE_DEGREE_TARGET
  ON ONE_DEGREE_TARGET.uid = PEOPLE.uid
WHERE ad_action = "clicked" AND buy_action = "bought_car"
GROUP BY RACE
order by TOTAL;

-- COMMAND ----------

SELECT PEOPLE.Religion, COUNT(*) AS TOTAL
FROM PEOPLE
JOIN ONE_DEGREE_TARGET
  ON ONE_DEGREE_TARGET.uid = PEOPLE.uid
WHERE ad_action = "clicked" AND buy_action = "bought_car"
GROUP BY Religion
ORDER BY TOTAL;

-- COMMAND ----------

SELECT PEOPLE.Ethnicity, COUNT(*) AS TOTAL
FROM PEOPLE
JOIN ONE_DEGREE_TARGET
  ON ONE_DEGREE_TARGET.uid = PEOPLE.uid
WHERE ad_action = "clicked" AND buy_action = "bought_car"
GROUP BY Ethnicity
ORDER BY TOTAL;

-- COMMAND ----------

select `Family Income Detector` from PEOPLE limit 10;


-- COMMAND ----------

SELECT
  CASE WHEN (`Family Income Detector` >= 5000 AND `Family Income Detector` <= 100000) THEN '5000 - 100,000'
       WHEN (`Family Income Detector` >= 100001 AND `Family Income Detector` <= 200000) THEN '100,001 - 200,000'
       WHEN (`Family Income Detector` >= 200001 AND `Family Income Detector` <= 300000) THEN '200,001 - 300,000'
       WHEN (`Family Income Detector` >= 300001 AND `Family Income Detector` <= 400000) THEN '300,001 - 400,000'
       WHEN (`Family Income Detector` >= 400001 AND `Family Income Detector` <= 500000) THEN '400,001 - 500,000'
       ELSE 'Unknown'
  END Income,
  COUNT(*) AS INCOME_BUCKET
FROM PEOPLE
JOIN ONE_DEGREE_TARGET
  ON ONE_DEGREE_TARGET.uid = PEOPLE.uid
WHERE ad_action = "clicked" AND buy_action = "bought_car"
GROUP BY 1
ORDER BY INCOME_BUCKET;

  

-- COMMAND ----------

SELECT 
  CASE WHEN (`Birth Year` >= 1800 AND `Birth Year` <= 1925) THEN '1800 - 1925'
       WHEN (`Birth Year` >= 1926 AND `Birth Year` <= 1950) THEN '1926 - 1950'
       WHEN (`Birth Year` >= 1951 AND `Birth Year` <= 1972) THEN '1951 - 1972'
       WHEN (`Birth Year` >= 1972 AND `Birth Year` <= 1998) THEN '1972 - 1998'
       ELSE 'UNKNOWN'
   END Birth_Year,
   Count(*) As Age_Group
 FROM PEOPLE
 JOIN ONE_DEGREE_TARGET
   ON ONE_DEGREE_TARGET.uid = PEOPLE.uid
 WHERE ad_action = "clicked" AND buy_action = "bought_car"
 GROUP BY 1;

-- COMMAND ----------

SELECT edges, COUNT(*) AS TOTAL
FROM GRAPH
JOIN ONE_DEGREE_TARGET
  ON ONE_DEGREE_TARGET.uid = GRAPH.source_uid
WHERE ad_action = "clicked" AND buy_action = "bought_car"
GROUP BY 1;

-- COMMAND ----------

SELECT edges, COUNT(*) AS TOTAL
FROM GRAPH
JOIN ONE_DEGREE_TARGET
  ON ONE_DEGREE_TARGET.uid = GRAPH.source_uid
WHERE ad_action = "clicked" AND buy_action is null
GROUP BY 1;

-- COMMAND ----------

SELECT edges, COUNT(*) AS TOTAL
FROM GRAPH
JOIN ONE_DEGREE_TARGET
  ON ONE_DEGREE_TARGET.uid = GRAPH.source_uid
WHERE ad_action = "did_not_click" AND buy_action = "bought_car"
GROUP BY 1;

-- COMMAND ----------

SELECT edges, COUNT(*) AS TOTAL
FROM GRAPH
JOIN ONE_DEGREE_TARGET
  ON ONE_DEGREE_TARGET.uid = GRAPH.source_uid
WHERE ad_action = "did_not_click" AND buy_action is Null
GROUP BY 1;

-- COMMAND ----------

SELECT COUNT(*) 
FROM GRAPH
JOIN RECENT_PURCHASES
  ON GRAPH.sink_uid = RECENT_PURCHASES.uid;
  
