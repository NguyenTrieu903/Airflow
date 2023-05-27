from airflow.operators.dummy_operator import DummyOperator

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.common.constant import DUMMY_TASK_TYPE


class DummyTaskFactory(AbstractTaskFactory):

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[DUMMY_TASK_TYPE] = DummyTaskFactory.create_task
            return func(self)
        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        task = DummyOperator(dag=dag, **params)
        return task, task
