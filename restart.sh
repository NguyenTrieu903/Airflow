#!/usr/bin/env bash

#cd /home/nhattrieu/Documents/airflow_workspace/airflow_env/bin
#source activate

export AIRFLOW_DAGS_DS_CONFIG_FILE=/home/nhattrieu/airflow/dags/configure_bash/config_tasks_20230531113553.json
export AIRFLOW_DAGS_DE_CONFIG_FILE=/home/nhattrieu/airflow/dags/configure_bash/config_tasks_20230531113553.json

cd /home/nhattrieu/airflow
ps -aux | grep scheduler | awk '{system("kill -9 "$2)}'
rm airflow-scheduler.pid
airflow scheduler -D

ps -aux | grep airflow-webserver | awk '{system("kill -9 "$2)}'
rm airflow-webserver.pid
rm airflow-webserver-monitor.pid
airflow webserver -D