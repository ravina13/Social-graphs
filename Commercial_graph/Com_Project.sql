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


