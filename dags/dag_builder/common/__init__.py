from dag_builder.builder.json_config_reader import JsonConfigReader
import os

ds_config = None
de_config = None

ds_config_file = os.environ.get('AIRFLOW_DAGS_DS_CONFIG_FILE')
if ds_config_file is not None:
    ds_config = JsonConfigReader.read(ds_config_file)

de_config_file = os.environ.get('AIRFLOW_DAGS_DE_CONFIG_FILE')
if de_config_file is not None:
    de_config = JsonConfigReader.read(de_config_file)
