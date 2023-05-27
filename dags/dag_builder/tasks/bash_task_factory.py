from airflow.operators.bash_operator import BashOperator

from dag_builder.common.constant import COMMAND_TEMPLATE, OPTIONS, COMMAND_PARAMS, NAME, BASH_TASK_TYPE
from dag_builder.common import de_config

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.tasks.utils.task_utils import ignore_failed, local_semaphore


class BashTaskFactory(AbstractTaskFactory):

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[BASH_TASK_TYPE] = BashTaskFactory.create_task
            return func(self)
        return wrap_func

    @classmethod
    @local_semaphore
    @ignore_failed
    def create_task(cls, dag, task_config, params):
        if JsonConfigReader.get_property(task_config, COMMAND_TEMPLATE) is None:
            print('None roi ')
        # if de_config[JsonConfigReader.get_property(task_config, COMMAND_TEMPLATE)] is None:
        #     print("None roi")
        command_template = " ".join(de_config[JsonConfigReader.get_property(task_config, COMMAND_TEMPLATE)])
        command_params = JsonConfigReader.get_property(task_config, "{options}.{command_params}"
                                                       .format(options=OPTIONS, command_params=COMMAND_PARAMS))
        command = command_template.format(**command_params)
        task = BashOperator(dag=dag, task_id=JsonConfigReader.get_property(task_config, NAME), bash_command=command, **params)
        return task, task
