from airflow.operators.python_operator import PythonOperator

from dag_builder.contrib.dataset import HdfsDataset

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.common.constant import OPTIONS, PATH, FLAG, \
    PYTHON_GET_LATEST_PATH_TASK_TYPE, NAME
from dag_builder.builder.json_config_reader import JsonConfigReader


class PythonGetLatestPathTaskFactory(AbstractTaskFactory):

    @staticmethod
    def get_latest_path(task_config, **kwargs):
        path = JsonConfigReader.get_property(task_config, "{options}.{path}".format(options=OPTIONS, path=PATH))
        flag = JsonConfigReader.get_property(task_config, "{options}.{flag}".format(options=OPTIONS, flag=FLAG))

        latest_path = HdfsDataset(path=path, flag=flag).get_latest_of_directory()
        latest_date = latest_path.split("=")[1]
        return "{date}".format(date=latest_date)

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[PYTHON_GET_LATEST_PATH_TASK_TYPE] = PythonGetLatestPathTaskFactory.create_task
            return func(self)

        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        task_params = {"task_config": task_config}
        task = PythonOperator(python_callable=PythonGetLatestPathTaskFactory.get_latest_path,
                              task_id=JsonConfigReader.get_property(task_config, NAME),
                              provide_context=True,
                              dag=dag, op_kwargs=task_params, **params)
        return task, task
