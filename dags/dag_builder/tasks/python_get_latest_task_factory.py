from datetime import date

from airflow.operators.python_operator import PythonOperator

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.common.constant import OPTIONS, PATH, FREQUENCY, START_DATE, FLAG, PYTHON_GET_LATEST_TASK_TYPE, NAME, \
    REF_PATH
from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.contrib.dataset import HdfsDataset


class PythonGetLatestTaskFactory(AbstractTaskFactory):

    @staticmethod
    def get_latest(task_config, **kwargs):
        path = JsonConfigReader.get_property(task_config, "{options}.{path}".format(options=OPTIONS, path=PATH))
        frequency = JsonConfigReader.get_property(task_config,
                                                  "{options}.{frequency}".format(options=OPTIONS, frequency=FREQUENCY))
        start_date = JsonConfigReader.get_property(task_config, "{options}.{start_date}".format(options=OPTIONS,
                                                                                                start_date=START_DATE))
        flag = JsonConfigReader.get_property(task_config, "{options}.{flag}".format(options=OPTIONS, flag=FLAG))
        d = kwargs.get('ds')

        latest_path = HdfsDataset(path=path,
                                  frequency=frequency,
                                  start_date=date(int(start_date.split("-")[0]), int(start_date.split("-")[1]),
                                                  int(start_date.split("-")[2])),
                                  end_date=d,
                                  flag=flag).get_latest()
        accepted_date = latest_path.split("=")[1]
        return "{latest_path}\\\\={date}".format(latest_path=JsonConfigReader.get_property(task_config,
                                                                                           "{options}.{ref_path}".format(
                                                                                               options=OPTIONS,
                                                                                               ref_path=REF_PATH),
                                                                                           latest_path.split("=")[0]),
                                                 date=accepted_date)

    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[PYTHON_GET_LATEST_TASK_TYPE] = PythonGetLatestTaskFactory.create_task
            return func(self)

        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        task_params = {"task_config": task_config}
        task = PythonOperator(python_callable=PythonGetLatestTaskFactory.get_latest,
                              task_id=JsonConfigReader.get_property(task_config, NAME),
                              provide_context=True,
                              dag=dag, op_kwargs=task_params, **params)
        return task, task
