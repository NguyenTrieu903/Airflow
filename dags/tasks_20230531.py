from datetime import timedelta
import datetime

from dag_builder.builder.graph_flow_builder import GraphFlowBuilder

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('tasks_20230531',
          default_args=default_args,
          schedule_interval=timedelta(1),
          max_active_runs=1,
          start_date=datetime.datetime(2022, 5, 5),
          end_date=datetime.datetime(2023, 12, 31))

config_file = "/home/nhattrieu/airflow/dags/json/tasks_20230531113552.json"
print("===============Processing: {config_file}================".format(config_file=config_file))

flow_builder = GraphFlowBuilder(dag=dag, task_id="tasks_20230531", flow_file_name=config_file)
check_daily_datasets = flow_builder.create_flow()