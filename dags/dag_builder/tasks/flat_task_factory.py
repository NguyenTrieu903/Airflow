from dag_builder.common.constant import FLAT_TASK_TYPE, NAME, CONFIG_FILE
from dag_builder.builder.graph_flow_builder import GraphFlowBuilder

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.tasks.utils.task_utils import local_semaphore


class FlatTaskFactory(AbstractTaskFactory):

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[FLAT_TASK_TYPE] = FlatTaskFactory.create_task
            return func(self)
        return wrap_func

    @classmethod
    @local_semaphore
    def create_task(cls, dag, task_config, params):
        flow_builder = GraphFlowBuilder(dag=dag,
                                        task_id=JsonConfigReader.get_property(task_config, NAME),
                                        flow_file_name=JsonConfigReader.get_property(task_config, CONFIG_FILE))
        tasks = flow_builder.create_flow()
        return tasks
