from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from dag_builder.common.constant import IGNORE_FAILED, NAME, SEMAPHORE, INIT_VALUE
from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.contrib.local_semaphore_operator import LocalSemaphoreAcquire, LocalSemaphoreRelease


def ignore_failed(func):
    def ignore_failed_wrap_func(cls, dag, task_config, params):
        tasks = func(cls=cls, dag=dag, task_config=task_config, params=params)
        is_ignore_failed = JsonConfigReader.get_property(task_config, IGNORE_FAILED, False)

        if is_ignore_failed:
            dummy = DummyOperator(dag=dag, task_id="dummy-{name}".format(name=JsonConfigReader.get_property(task_config, NAME)),
                                  trigger_rule=TriggerRule.ALL_DONE)
            tasks[1] >> dummy
            return tasks[0], dummy
        else:
            return tasks

    return ignore_failed_wrap_func


def local_semaphore(func):
    def local_semaphore_wrap_func(cls, dag, task_config, params):
        tasks = func(cls=cls, dag=dag, task_config=task_config, params=params)
        semaphore = JsonConfigReader.get_property(task_config, SEMAPHORE)
        if semaphore:
            acquire_operator = LocalSemaphoreAcquire(semaphore_name=semaphore[NAME], init_value=semaphore[INIT_VALUE],
                                                     dag=dag, task_id="{task}-acquire-{name}".format(task=JsonConfigReader.get_property(task_config, NAME), name=semaphore[NAME]))
            dummy_after_acquire = DummyOperator(dag=dag, task_id="{task}-after-acquire-{name}".format(task=JsonConfigReader.get_property(task_config, NAME), name=semaphore[NAME]))
            dummy_before_release = DummyOperator(dag=dag
                                                 , task_id="{task}-before-release-{name}".format(task=JsonConfigReader.get_property(task_config, NAME), name=semaphore[NAME])
                                                 , trigger_rule=TriggerRule.ALL_DONE)
            release_operator = LocalSemaphoreRelease(semaphore_name=semaphore[NAME]
                                                     , init_value=semaphore[INIT_VALUE]
                                                     , dag=dag
                                                     , task_id="{task}-release-{name}".format(task=JsonConfigReader.get_property(task_config, NAME), name=semaphore[NAME])
                                                     , trigger_rule=TriggerRule.ALL_SUCCESS)

            acquire_operator >> tasks[0]
            acquire_operator >> dummy_after_acquire >> release_operator
            tasks[1] >> dummy_before_release >> release_operator
            return acquire_operator, release_operator
        else:
            return tasks

    return local_semaphore_wrap_func

