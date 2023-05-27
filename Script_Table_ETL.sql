create table ETL(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	name varchar(100) not null unique
)

create table Step(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	etl_id int,	
	name varchar(100) not null,
	input_format varchar(100) not null,
	output_format varchar(100) null,
	CONSTRAINT fk_step
      FOREIGN KEY(etl_id) 
	  REFERENCES ETL(id)
)

create table options_input(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	step_id int null,
	STREAMING BOOLEAN null,
	CONSTRAINT fk_options_input_step
      FOREIGN KEY(step_id) 
	  REFERENCES Step(id)
)

create table options_output(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	step_id int,
	path varchar(50) null,
	output_mode varchar(50) null,
	partition_cols varchar(50) null,
	CONSTRAINT fk_options_output
      FOREIGN KEY(step_id) 
	  REFERENCES Step(id)
)

create table actions (
	id int GENERATED ALWAYS AS IDENTITY primary key,
	step_id int null,
	name varchar(50) not null,
	CONSTRAINT fk_actions_step
      FOREIGN KEY(step_id) 
	  REFERENCES Step(id)
)

create table option_actions(
	id int GENERATED ALWAYS AS IDENTITY primary key,
	action_id int not null,
	exprs varchar[] null,
	partitions int null,
	other_dataset varchar(50) null,
	cols varchar[] null, 
	join_type varchar(50) null,
	id_rename varchar(50) null,
	CONSTRAINT option_actions
      FOREIGN KEY(action_id) 
	  REFERENCES actions(id)
)