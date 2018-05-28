drop database product_project;
create database product_project;
use product_project;
create table if not exists raw_data(
ac_sink varchar (50),
ac_source varchar(50),
sink_gender varchar (50),
sink_YOB int (50),
sink_race varchar (50),
source_gender varchar (50),
source_YOB int (50),
source_race varchar(50)
);
LOAD DATA LOCAL INFILE 'C:\\Users\\ravin\\Desktop\\Apple_cart\\product_intern_sample_graph.csv' INTO TABLE raw_data FIELDS TERMINATED BY ',';

create table unqiue_data
select distinct * from (
select
case when ac_sink <= ac_source then ac_sink else ac_source end as ac_sink,
case when ac_sink <= ac_source then ac_source else ac_sink end as ac_source,
case when ac_sink <= ac_source then sink_gender else source_gender end as sink_gender,
case when ac_sink <= ac_source then sink_YOB else source_YOB end as sink_YOB,
case when ac_sink <= ac_source then sink_race else source_race end as sink_race,
case when ac_sink <= ac_source then source_gender else sink_gender end as source_gender,
case when ac_sink <= ac_source then source_YOB else sink_YOB end as source_YOB,
case when ac_sink <= ac_source then source_race else sink_race end as source_race
from raw_data
) x;

-- returns 17019 rows

---------------------------------
source C:\Users\ravin\Desktop\Apple_cart\ravina_ddl.sql
select 
sum(case when sink_gender = source_gender then 1 else 0 end) as same_gender,
sum(case when sink_gender = source_gender then 0 else 1 end) as different_gender,
count(*) as total_relations
from raw_data;

-- number of same gender and different gender relations
----------------------------------

select 
sum(case when sink_gender = source_gender then 1 else 0 end) as same_gender,
sum(case when sink_gender = source_gender then 0 else 1 end) as different_gender,
count(*) as total_relations,
sum(case when sink_gender = source_gender then 1 else 0 end)/count(*)*100 as same_gender_percentage,
sum(case when sink_gender = source_gender then 0 else 1 end)/count(*)*100 as different_gender_percentage
from raw_data;

-- percentage of same gender and different gender relations

----------------------------------

select 
sum(case when sink_YOB = source_YOB then 1 else 0 end) as same_YOB,
sum(case when sink_YOB = source_YOB then 0 else 1 end) as different_YOB,
count(*) as total_relations,
sum(case when sink_YOB = source_YOB then 1 else 0 end)/count(*)*100 as same_YOB_percentage,
sum(case when sink_YOB = source_YOB then 0 else 1 end)/count(*)*100 as different_YOB_percentage
from raw_data;

-- percentage of same age and different age relations (if we only care about same and different ages then we dont really need to calculate the age)

-----------------------------------

select 
sum(case when sink_race = source_race then 1 else 0 end) as same_race,
sum(case when sink_race = source_race then 0 else 1 end) as different_race,
count(*) as total_relations,
sum(case when sink_race = source_race then 1 else 0 end)/count(*)*100 as same_race_percentage,
sum(case when sink_race = source_race then 0 else 1 end)/count(*)*100 as different_race_percentage
from raw_data
where sink_race != 'Unknown'
and source_race != 'Unknown';

-- percentage of same race and different race relations

-----------------------------------

select 
sink_race as race, 
sum(case when sink_race = source_race then 1 else 0 end) as same_race_connections,
count(*) as total_connections,
sum(case when sink_race = source_race then 1 else 0 end)/count(*)*100 as same_race_percentage
from raw_data
group by sink_race;

-- how likely each race is to have a connection within their race

-----------------------------------

select 
sink_race as race, 
sink_gender as gender,
case 
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 0 and 19 then '0_19'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 20 and 39 then '20_39'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 40 and 59 then '40_59'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 60 and 79 then '60_79'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) >= 80 then '80+'
end as age,
count(*) as connections
from raw_data
group by 1, 2, 3
order by 4;

-- to demonstrate which demographics are over or under represented in the dataset

-----------------------------------

select 
sum(case when sink_age_buckets = source_age_buckets then 1 else 0 end) as same_age_buckets,
sum(case when sink_age_buckets = source_age_buckets then 0 else 1 end) as different_age_buckets,
count(*) as total_relations,
sum(case when sink_age_buckets = source_age_buckets then 1 else 0 end)/count(*)*100 as same_age_buckets_percentage,
sum(case when sink_age_buckets = source_age_buckets then 0 else 1 end)/count(*)*100 as different_age_buckets_percentage
from 
(
select 
*,
case 
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 0 and 19 then '0_19'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 20 and 39 then '20_39'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 40 and 59 then '40_59'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 60 and 79 then '60_79'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) >= 80 then '80+'
end as sink_age_buckets,
case 
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(source_YOB, '-01-01')), CURDATE()) between 0 and 19 then '0_19'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(source_YOB, '-01-01')), CURDATE()) between 20 and 39 then '20_39'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(source_YOB, '-01-01')), CURDATE()) between 40 and 59 then '40_59'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(source_YOB, '-01-01')), CURDATE()) between 60 and 79 then '60_79'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(source_YOB, '-01-01')), CURDATE()) >= 80 then '80+'
end as source_age_buckets
from raw_data
) x;

-- same and different age analysis with age buckets

---------------------------------

-- EXTRA STUFF

---------------------------------

select 
sink_race,
source_race,
count(*) as connections
from raw_data
group by 1, 2
order by 3 desc

-- number of connections between different race pairs 
-- eg. white peole have lots of connections amongst themselves while asians and blacks dont have lots of connectiosns with each other

----------------------------------


