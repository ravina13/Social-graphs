drop database product_project;
create database product_project;
use product_project;

source C:\Users\ravin\Desktop\Apple_cart\ravina_ddl.sql
select count(*) from raw_data;

select count(distinct sink_gender) 
from raw_data ;

select count(sink_gender) 
from raw_data
where sink_race = 'White';

select count(ac_sink)
from raw_data
where not sink_race = 'Unknown';

select * from raw_data 
order by sink_YOB limit 10;

select ac_sink, sink_YOB, length(ac_sink) 
from raw_data 
order by sink_YOB DESC limit 50;

select distinct left(ac_sink,1), left(ac_source,1)
from raw_data
order by left(ac_sink) desc limit 100;

select count(distinct ac_sink, sink_gender, CONCAT(ac_sink,"",ac_source)) as ID
from raw_data limit 100;

select 
sum(case when sink_gender = source_gender then 1 else 0 end) as same_gender,
sum(case when sink_gender = source_gender then 0 else 1 end) as different_gender,
count(*) as total_relations
from raw_data;

select 
sum(case when sink_gender = source_gender then 1 else 0 end) as same_gender,
sum(case when sink_gender = source_gender then 0 else 1 end) as different_gender,
count(*) as total_relations,
sum(case when sink_gender = source_gender then 1 else 0 end)/count(*)*100 as same_gender_percentage,
sum(case when sink_gender = source_gender then 0 else 1 end)/count(*)*100 as different_gender_percentage
from raw_data;

select 
sum(case when sink_YOB = source_YOB then 1 else 0 end) as same_YOB,
sum(case when sink_YOB = source_YOB then 0 else 1 end) as different_YOB,
count(*) as total_relations,
sum(case when sink_YOB = source_YOB then 1 else 0 end)/count(*)*100 as same_YOB_percentage,
sum(case when sink_YOB = source_YOB then 0 else 1 end)/count(*)*100 as different_YOB_percentage
from raw_data;

select 
sum(case when sink_race = source_race then 1 else 0 end) as same_race,
sum(case when sink_race = source_race then 0 else 1 end) as different_race,
count(*) as total_relations,
sum(case when sink_race = source_race then 1 else 0 end)/count(*)*100 as same_race_percentage,
sum(case when sink_race = source_race then 0 else 1 end)/count(*)*100 as different_race_percentage
from raw_data
where sink_race != 'Unknown'
and source_race != 'Unknown';

select 
sink_race as race, 
sum(case when sink_race = source_race then 1 else 0 end) as same_race_connections,
count(*) as total_connections,
sum(case when sink_race = source_race then 1 else 0 end)/count(*)*100 as same_race_percentage
from raw_data
group by sink_race;

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


select * , 
case 
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 0 and 19 then '0_19'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 20 and 39 then '20_39'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 40 and 59 then '40_59'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 60 and 79 then '60_79'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) >= 80 then '80+'
end as age_bucket,
count(*) as connections,
(select count(*) form raw_data ) as total
from raw_data
group by age_bucket;

select * , 
case 
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 0 and 19 then '0_19'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 20 and 39 then '20_39'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 40 and 59 then '40_59'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) between 60 and 79 then '60_79'
when TIMESTAMPDIFF(YEAR, DATE(CONCAT(sink_YOB, '-01-01')), CURDATE()) >= 80 then '80+'
end as age_bucket,
count(*) as connections,
count(*)/x.total*100 as Percent
from raw_data, (select count(*) as total from raw_data ) x 
group by age_bucket;