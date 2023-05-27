from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from dag_builder.common.constant import NAME, DS_OUTPUTS, DS_CHECK_OUTPUT_COMMAND, OPTIONS, HAS_CHECK_OUTPUT, \
    DS_CODE_TASK_TYPE, DS_RUN_COMMAND, PARALLEL

from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.tasks.utils.task_utils import ignore_failed, local_semaphore

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.common import ds_config


def create_check_output(func):
    def check_output(dag, task_config, params):
        date = "{{ next_execution_date.strftime('%Y%m%d') }}"
        try:
            if len(ds_config[DS_OUTPUTS][JsonConfigReader.get_property(task_config, NAME)]) == 1:
                path = ds_config[DS_OUTPUTS][JsonConfigReader.get_property(task_config, NAME)][0][0].format(date=date)
                nod = ds_config[DS_OUTPUTS][JsonConfigReader.get_property(task_config, NAME)][0][1]
                nof = ds_config[DS_OUTPUTS][JsonConfigReader.get_property(task_config, NAME)][0][2]
                command = ds_config[DS_CHECK_OUTPUT_COMMAND]
                params["priority_weight"] = 10
                bash_command = command.format(path=path, nod=nod, nof=nof)
                task = BashOperator(task_id="check-output-{name}".format(name=JsonConfigReader.get_property(task_config, NAME)),
                                    bash_command=bash_command,
                                    dag=dag,
                                    **params)
                return task, task
            else:
                t_dummy = DummyOperator(task_id="start-check-output-{name}".format(name=JsonConfigReader.get_property(task_config, NAME)),
                                        dag=dag)
                t_e_dummy = DummyOperator(task_id="end-check-output-{name}".format(name=JsonConfigReader.get_property(task_config, NAME)),
                                          dag=dag)
                for i in range(len(ds_config[DS_OUTPUTS][JsonConfigReader.get_property(task_config, NAME)])):
                    op = ds_config[DS_OUTPUTS][JsonConfigReader.get_property(task_config, NAME)][i]
                    path = op[0].format(date=date)
                    nod = op[1]
                    nof = op[2]
                    command = ds_config[DS_CHECK_OUTPUT_COMMAND]
                    bash_command = command.format(path=path, nod=nod, nof=nof)
                    t_dummy >> BashOperator(task_id="check-output-{name}-{i}".format(name=JsonConfigReader.get_property(task_config, NAME), i=str(i)),
                                            bash_command=bash_command,
                                            dag=dag,
                                            **params) >> t_e_dummy
                return t_dummy, t_e_dummy
        except KeyError:
            dummy = DummyOperator(dag=dag, task_id="dummy-check-output-{name}".format(name=JsonConfigReader.get_property(task_config, NAME)))
            return dummy, dummy

    def wrap_func(cls, dag, task_config, params):
        tasks = func(cls=cls, dag=dag, task_config=task_config, params=params)
        has_check_output = JsonConfigReader.get_property(task_config, "{options}.{has_check_output}"
                                                         .format(options=OPTIONS, has_check_output=HAS_CHECK_OUTPUT), False)

        if has_check_output:
            check_output_task = check_output(dag=dag, task_config=task_config, params=params)
            tasks[1] >> check_output_task[0]
            return tasks[0], check_output_task[1]
        else:
            return tasks

    return wrap_func


class DSTaskFactory(AbstractTaskFactory):
    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[DS_CODE_TASK_TYPE] = DSTaskFactory.create_ds_task
            return func(self)
        return wrap_func

    @classmethod
    def create_ds_task(cls, dag, task_config, params):
        return DSTaskFactory.create_task(dag=dag, task_config=task_config, params=params)

    @classmethod
    @local_semaphore
    @ignore_failed
    @create_check_output
    def create_task(cls, dag, task_config, params):
        date = "{{ next_execution_date.strftime('%Y%m%d') }}"
        command = ds_config[DS_RUN_COMMAND]
        parallel = JsonConfigReader.get_property(task_config, "{options}.{parallel}".format(options=OPTIONS, parallel=PARALLEL), 0)
        if parallel == 0:
            start = 'all'
            bash_command = command.format(name=JsonConfigReader.get_property(task_config, NAME), date=date, start=start, mod="0")
            task = BashOperator(task_id="""run-{name}-{mod}""".format(name=JsonConfigReader.get_property(task_config, NAME), mod=start),
                                bash_command=bash_command,
                                dag=dag,
                                **params)
            return task, task
        else:
            start_dummy = DummyOperator(task_id="""start-{name}""".format(name=JsonConfigReader.get_property(task_config, NAME)))
            end_dummy = DummyOperator(task_id="""end-{name}""".format(name=JsonConfigReader.get_property(task_config, NAME)))
            mod = JsonConfigReader.get_property(task_config, "{options}.{parallel}".format(options=OPTIONS, parallel=PARALLEL))
            for start in range(mod):
                bash_command = command.format(name=JsonConfigReader.get_property(task_config, NAME), date=date, start=str(start), mod=str(mod))
                start_dummy >> BashOperator(
                    task_id="""run-{name}-{start}""".format(name=JsonConfigReader.get_property(task_config, NAME), start=start),
                    bash_command=bash_command,
                    dag=dag,
                    **params) >> end_dummy
            return start_dummy, end_dummy
