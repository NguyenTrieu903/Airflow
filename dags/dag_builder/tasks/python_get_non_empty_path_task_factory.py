from airflow.operators.python_operator import PythonOperator


from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.common.constant import PYTHON_GET_NON_EMPTY_PATH_TASK_TYPE, NAME, OPTIONS, PATH, FLAG, MIN_SIZE, LEVEL
from dag_builder.contrib.dataset import HdfsDataset
from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory


class PythonGetNonEmptyTaskFactory(AbstractTaskFactory):

    @staticmethod
    def get_non_empty_path(task_config, **kwargs):
        path = JsonConfigReader.get_property(task_config, "{options}.{path}".format(options=OPTIONS, path=PATH))
        flag = JsonConfigReader.get_property(task_config, "{options}.{flag}".format(options=OPTIONS, flag=FLAG))
        min_size = JsonConfigReader.get_property(task_config, "{options}.{size}".format(options=OPTIONS, size=MIN_SIZE))
        level = JsonConfigReader.get_property(task_config, "{options}.{level}".format(options=OPTIONS, level=LEVEL))
        non_empty_path = HdfsDataset(path=path, flag=flag).get_non_empty_path(level=level, size=min_size)
        return non_empty_path

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[PYTHON_GET_NON_EMPTY_PATH_TASK_TYPE] = PythonGetNonEmptyTaskFactory.create_task
            return func(self)

        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        task_params = {"task_config": task_config}
        task = PythonOperator(python_callable=PythonGetNonEmptyTaskFactory.get_non_empty_path,
                              task_id=JsonConfigReader.get_property(task_config, NAME),
                              provide_context=True,
                              dag=dag, op_kwargs=task_params, **params)
        return task, task
