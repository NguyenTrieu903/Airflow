from dag_builder.tasks.python_get_latest_path_task_factory import PythonGetLatestPathTaskFactory

from dag_builder.common.constant import PRIORIRTY_WEIGHT, TASK_TYPE, POOL, DEPENDS_ON_PAST, TASK_CONCURRENCY
from dag_builder.tasks.flat_task_factory import FlatTaskFactory
from dag_builder.tasks.get_current_date_factory import PythonGetCurrentDateTaskFactory
from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.tasks.python_get_first_path_task_factory import PythonGetFirstPathTaskFactory
from dag_builder.tasks.python_get_latest_task_factory import PythonGetLatestTaskFactory
from dag_builder.tasks.python_get_non_empty_path_task_factory import PythonGetNonEmptyTaskFactory
from dag_builder.tasks.sub_dag_task_factory import SubDagTaskFactory

from dag_builder.tasks.bash_task_factory import BashTaskFactory

from dag_builder.tasks.ds_task_factory import DSTaskFactory
from dag_builder.tasks.dummy_task_factory import DummyTaskFactory
from dag_builder.tasks.sensor_task_factory import HdfsSensorTaskFactory, TimeSensorTaskFactory, BashSensorTaskFactory, \
    GCSSensorTaskFactory


class TaskFactory:
    def __init__(self):
        self.tasks = {}
        self.intercept()

    def create_task(self, dag, task_config):
        params = TaskFactory.build_params(task_config)
        return self.tasks[JsonConfigReader.get_property(task_config, TASK_TYPE)](dag, task_config, params)

    @staticmethod
    @HdfsSensorTaskFactory.build_params_decorator
    def build_params(config):
        params = {}
        # priority_weight
        try:
            params[PRIORIRTY_WEIGHT] = config[PRIORIRTY_WEIGHT]
        except KeyError:
            params[PRIORIRTY_WEIGHT] = 1

        # pool
        try:
            params[POOL] = config[POOL]
        except KeyError as e:
            pass

        # depends_on_past
        try:
            params[DEPENDS_ON_PAST] = config[DEPENDS_ON_PAST]
        except KeyError as e:
            pass

        # task_concurrency
        try:
            params[TASK_CONCURRENCY] = config[TASK_CONCURRENCY]
        except KeyError as e:
            pass

        return params

    @PythonGetNonEmptyTaskFactory.intercept_task
    @PythonGetLatestPathTaskFactory.intercept_task
    @PythonGetCurrentDateTaskFactory.intercept_task
    @PythonGetFirstPathTaskFactory.intercept_task
    @GCSSensorTaskFactory.intercept_task
    @BashSensorTaskFactory.intercept_task
    @DummyTaskFactory.intercept_task
    @DSTaskFactory.intercept_task
    @HdfsSensorTaskFactory.intercept_task
    @TimeSensorTaskFactory.intercept_task
    @BashTaskFactory.intercept_task
    @SubDagTaskFactory.intercept_task
    @PythonGetLatestTaskFactory.intercept_task
    @FlatTaskFactory.intercept_task
    def intercept(self):
        return self
