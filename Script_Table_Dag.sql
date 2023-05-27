create table ScheduleDag (
	id int GENERATED ALWAYS AS IDENTITY primary key,
	name varchar(100) not null	
)

create table task(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	name varchar(100) not null,
	dag_id int,
	task_type varchar(100),
	priority_weight int,
	pool varchar(100),
	upstreams varchar[],
	filepath varchar(100),
	flag varchar(100),
	recursive BOOLEAN,
	min_size int,
	ignore_failed BOOLEAN,
	command_template varchar(100),
	CONSTRAINT fk_task
      FOREIGN KEY(dag_id) 
	  REFERENCES ScheduleDag(id)
)

create table option(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	task_id int not null,
	number_of_days int null,
	interval int null,
	date varchar(100) null,
	format_date varchar(100),
	format_previous_date varchar(100),
	CONSTRAINT fk_option
      FOREIGN KEY(task_id) 
	  REFERENCES task(id)
)

create table command(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	option_id int not null,
	dataset varchar(100),
	date varchar(100),
	previous_date varchar(100),
	CONSTRAINT fk_command
      FOREIGN KEY(option_id) 
	  REFERENCES option(id)
)


create table dag_file_mapping(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	dag_id int UNIQUE,
	config_name varchar(100),
	CONSTRAINT fk_dag_file_mapping
      FOREIGN KEY(dag_id) 
	  REFERENCES ScheduleDag(id)
)

ALTER TABLE ScheduleDag ADD UNIQUE (name)

drop table dag_file_mapping
drop table command
drop table option
drop table task
drop table ScheduleDag

