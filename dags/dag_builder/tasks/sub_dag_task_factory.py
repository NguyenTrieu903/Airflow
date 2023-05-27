from airflow import DAG
# from airflow.executors import LocalExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.operators.subdag_operator import SubDagOperator

from dag_builder.common.constant import SUB_DAG, NAME, CONFIG_FILE, POOL
from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.tasks.utils.task_utils import ignore_failed, local_semaphore

from dag_builder.builder.graph_flow_builder import GraphFlowBuilder

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory


class SubDagTaskFactory(AbstractTaskFactory):
    @staticmethod
    def create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
        sub_dag = DAG(
            dag_id='{parent_name}.{children_name}'.format(parent_name=parent_dag_name, children_name=child_dag_name),
            schedule_interval=schedule_interval,
            start_date=start_date,
            concurrency=10
        )
        return sub_dag

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[SUB_DAG] = SubDagTaskFactory.create_task
            return func(self)
        return wrap_func

    @classmethod
    @ignore_failed
    @local_semaphore
    def create_task(cls, dag, task_config, params):

        sub_dag = SubDagTaskFactory.create_sub_dag(parent_dag_name=dag.dag_id,
                                                   child_dag_name=JsonConfigReader.get_property(task_config, NAME),
                                                   start_date=dag.start_date,
                                                   schedule_interval=dag.schedule_interval)

        config_file = JsonConfigReader.get_property(task_config, CONFIG_FILE)

        flow_builder = GraphFlowBuilder(dag=sub_dag, task_id=JsonConfigReader.get_property(task_config, NAME), flow_file_name=config_file)
        flow_builder.create_flow()
        task = SubDagOperator(
            subdag=sub_dag,
            pool=JsonConfigReader.get_property(task_config, POOL, "demo_pool"),
            task_id=JsonConfigReader.get_property(task_config, NAME),
            dag=dag,
            executor=LocalExecutor())
        return task, task
