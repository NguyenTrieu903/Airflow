from airflow.operators.python_operator import PythonOperator
from datetime import date

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.common.constant import PYTHON_GET_CURRENT_DATE, NAME
from dag_builder.builder.json_config_reader import JsonConfigReader


class PythonGetCurrentDateTaskFactory(AbstractTaskFactory):

    @staticmethod
    def get_current_date(**kwargs):
        today = date.today()
        return today.strftime("%Y%m%d")

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[PYTHON_GET_CURRENT_DATE] = PythonGetCurrentDateTaskFactory.create_task
            return func(self)
        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        task = PythonOperator(python_callable=PythonGetCurrentDateTaskFactory.get_current_date,
                              task_id=JsonConfigReader.get_property(task_config, NAME),
                              provide_context=True,
                              dag=dag, **params)
        return task, task

