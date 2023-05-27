from airflow.operators.python_operator import PythonOperator
from dag_builder.contrib.dataset import HdfsDataset

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.common.constant import PYTHON_GET_FIRST_PATH_TASK_TYPE, NAME, OPTIONS, PATH, FLAG
from dag_builder.builder.json_config_reader import JsonConfigReader


class PythonGetFirstPathTaskFactory(AbstractTaskFactory):

    @staticmethod
    def get_first_path(task_config, **kwargs):
        path = JsonConfigReader.get_property(task_config, "{options}.{path}".format(options=OPTIONS, path=PATH))
        flag = JsonConfigReader.get_property(task_config, "{options}.{flag}".format(options=OPTIONS, flag=FLAG))

        first_path = HdfsDataset(path=path, flag=flag).get_first()
        first_date = first_path.split("=")[1]
        return "{first_path}\\\\={date}".format(first_path=first_path.split("=")[0], date=first_date)

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[PYTHON_GET_FIRST_PATH_TASK_TYPE] = PythonGetFirstPathTaskFactory.create_task
            return func(self)

        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        task_params = {"task_config": task_config}
        task = PythonOperator(python_callable=PythonGetFirstPathTaskFactory.get_first_path,
                              task_id=JsonConfigReader.get_property(task_config, NAME),
                              provide_context=True,
                              dag=dag, op_kwargs=task_params, **params)
        return task, task
