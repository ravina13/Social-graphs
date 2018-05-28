drop database sample_graph;
create database sample_graph;
use sample_graph;
	create table if not exists full_data(
		ac_sinkid varchar (50),
		ac_sourceid varchar(50),
		gender_sink char (25),
		YOB_sink int (25),
		age_sink int (25),
		race_sink char (25),
		gender_source char (25),
		YOB_source int (25),
		age_source int (25),
		race_source char(25));
	select "Table created" as msg;
	create table if not exists AC_Sink(
	  	sink_no varchar (50),
	  	sink_gender char (25),
	  	sink_YOB int (25),
	  	sink_race char (25)
	  	) ;
	select "Table created" as msg;
	create table if not exists AC_Source(
	 	source_no varchar (50),
	  	source_gender char (25),
	  	source_YOB int (25),
	  	source_race char (25)
	 	);
	select "Table created" as msg;
	LOAD DATA LOCAL INFILE 'C:\\Users\\ravin\\Desktop\\Apple_cart\\product_intern_sample_graph.csv' INTO TABLE full_data FIELDS TERMINATED BY ',';
	LOAD DATA LOCAL INFILE 'C:\\Users\\ravin\\Desktop\\Apple_cart\\sample_sink.txt' INTO TABLE AC_Sink FIELDS TERMINATED BY ',';
	LOAD DATA LOCAL INFILE 'C:\\Users\\ravin\\Desktop\\Apple_cart\\sample_source.txt' INTO TABLE AC_Source FIELDS TERMINATED BY ',';
	select "Data Loaded" as msg;
	alter table AC_Sink
	add age int generated always as (2017-sink_YOB) Stored;
	alter table AC_Source
	add age_source int generated always as (2017-source_YOB) Stored;
	select "Age col added" as msg;
	select * from full_data limit 100;
	select * from AC_Sink limit 100;
	select * from AC_Source limit 100;

	select ac_sinkid, ac_sourceid,(concat (ac_sinkid, ac_sourceid)) as temp  
	from full_data
	where ascii(ac_sinkid) < ascii(ac_sourceid) 
	union
	select ac_sinkid, ac_sourceid,(concat (ac_sourceid, ac_sinkid)) as temp 
	from full_data ;
	where ascii(ac_sinkid) > ascii(ac_sourceid);

	select distinct temp from 
	(
		select ac_sinkid, ac_sourceid,(concat (ac_sinkid, ac_sourceid)) 
	from full_data
	where ascii(ac_sinkid) < ascii(ac_sourceid) 
	union
	select ac_sinkid, ac_sourceid,(concat (ac_sourceid, ac_sinkid)) 
	from full_data 
	where ascii(ac_sinkid) > ascii(ac_sourceid)
	);



