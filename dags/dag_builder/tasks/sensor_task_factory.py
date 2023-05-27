import datetime

from airflow.contrib.sensors.bash_sensor import BashSensor
from airflow.operators.dummy_operator import DummyOperator

from dag_builder.common.constant import TIME_SENSOR_TASK_TYPE, OPTIONS, ON_TIME, OFF_TIME, NAME, HDFS_SENSOR_TASK_TYPE, \
    HDFS_COUNT_SENSOR_TASK_TYPE, NUMBER_OF_DAYS, FILE_PATH, DIRECTORY_COUNT, FILE_COUNT, INTERVAL, FLAG, RECURSIVE, \
    MIN_SIZE, BASH_SENSOR_TASK_TYPE, COMMAND_TEMPLATE, COMMAND_PARAMS, GCS_SENSOR_TASK_TYPE, BUCKET, OBJECT, \
    GOOGLE_CLOUD_CONN_ID, DELEGATE_TO
from dag_builder.common import de_config
from dag_builder.builder.json_config_reader import JsonConfigReader
from dag_builder.contrib.time_sensor import TimeSensor

from dag_builder.tasks.utils.abstract_task_factory import AbstractTaskFactory
from dag_builder.contrib.hdfs_sensor import HdfsSensorGen, HdfsSensorWithCount


class TimeSensorTaskFactory(AbstractTaskFactory):
    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[TIME_SENSOR_TASK_TYPE] = TimeSensorTaskFactory.create_task
            return func(self)

        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        task = TimeSensor(on_time=datetime.time(
            JsonConfigReader.get_property(task_config, "{options}.{on_time}".format(options=OPTIONS, on_time=ON_TIME)),
            0),
                          off_time=datetime.time(JsonConfigReader.get_property(task_config,
                                                                               "{options}.{off_time}".format(
                                                                                   options=OPTIONS, off_time=OFF_TIME)),
                                                 0),
                          task_id="{name}".format(name=JsonConfigReader.get_property(task_config, NAME)),
                          dag=dag, **params)
        return task, task


class HdfsSensorTaskFactory(AbstractTaskFactory):
    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[HDFS_SENSOR_TASK_TYPE] = HdfsSensorTaskFactory.create_task
            self.tasks[HDFS_COUNT_SENSOR_TASK_TYPE] = HdfsSensorTaskFactory.create_hdfs_count_sensor
            return func(self)

        return wrap_func

    @classmethod
    def create_hdfs_count_sensor(cls, dag, task_config, params):
        days = JsonConfigReader.get_property(task_config, "{options}.{number_of_days}".format(options=OPTIONS,
                                                                                              number_of_days=NUMBER_OF_DAYS),
                                             0)
        if days == 0:
            file_path = "".join([JsonConfigReader.get_property(task_config, FILE_PATH)])
            sensor = HdfsSensorWithCount(
                directory_count=JsonConfigReader.get_property(task_config, DIRECTORY_COUNT),
                file_count=JsonConfigReader.get_property(task_config, FILE_COUNT),
                filepath=file_path.format(**(JsonConfigReader.get_property(task_config, OPTIONS))),
                dag=dag,
                hdfs_conn_id='hdfs_default',
                task_id='{name}'.format(name=JsonConfigReader.get_property(task_config, NAME)),
                **params)
            return sensor, sensor
        else:
            start_dummy = DummyOperator(dag=dag, task_id='start-{name}'.format(
                name=JsonConfigReader.get_property(task_config, NAME)))
            end_dummy = DummyOperator(dag=dag, task_id='end-{name}'.format(
                name=JsonConfigReader.get_property(task_config, NAME)))
            frequency = JsonConfigReader.get_property(task_config,
                                                      "{options}.{interval}".format(options=OPTIONS, interval=INTERVAL))
            for i in range(JsonConfigReader.get_property(task_config,
                                                         "{options}.{number_of_days}".format(options=OPTIONS,
                                                                                             number_of_days=NUMBER_OF_DAYS))):
                date = """{{ (next_execution_date - macros.timedelta(days=""" + str(
                    i * frequency) + """)).strftime('%Y%m%d') }}"""
                file_path = JsonConfigReader.get_property(task_config, FILE_PATH).format(date=date)
                start_dummy >> HdfsSensorWithCount(
                    directory_count=JsonConfigReader.get_property(task_config, DIRECTORY_COUNT),
                    file_count=JsonConfigReader.get_property(task_config, FILE_COUNT),
                    filepath=file_path,
                    dag=dag,
                    hdfs_conn_id='hdfs_default',
                    task_id='{name}-{i}'.format(name=JsonConfigReader.get_property(task_config, NAME), i=i),
                    **params
                ) >> end_dummy
            return start_dummy, end_dummy

    @classmethod
    def create_task(cls, dag, task_config, params):
        days = JsonConfigReader.get_property(task_config, "{options}.{number_of_days}".format(options=OPTIONS,
                                                                                              number_of_days=NUMBER_OF_DAYS),
                                             0)

        if days == 0:
            file_path = "".join([JsonConfigReader.get_property(task_config, FILE_PATH)])
            sensor = HdfsSensorGen(
                filepath=file_path.format(**(JsonConfigReader.get_property(task_config, OPTIONS))),
                dag=dag,
                hdfs_conn_id='hdfs_default',
                task_id='{name}'.format(name=JsonConfigReader.get_property(task_config, NAME)),
                **params
            )
            return sensor, sensor
        else:
            start_dummy = DummyOperator(dag=dag, task_id='start-{name}'.format(name=JsonConfigReader.get_property(task_config, NAME)))
            end_dummy = DummyOperator(dag=dag, task_id='end-{name}'.format(name=JsonConfigReader.get_property(task_config, NAME)))
            frequency = JsonConfigReader.get_property(task_config,
                                                      "{options}.{interval}".format(options=OPTIONS, interval=INTERVAL))
            for i in range(JsonConfigReader.get_property(task_config,
                                                         "{options}.{number_of_days}".format(options=OPTIONS,
                                                                                             number_of_days=NUMBER_OF_DAYS))):
                date = """{{ (next_execution_date - macros.timedelta(days=""" + str(
                    i * frequency) + """)).strftime('%Y%m%d') }}"""
                file_path = JsonConfigReader.get_property(task_config, FILE_PATH).format(date=date)
                start_dummy >> HdfsSensorGen(
                    filepath=file_path,
                    dag=dag,
                    hdfs_conn_id='hdfs_default',
                    task_id='{name}-{i}'.format(name=JsonConfigReader.get_property(task_config, NAME), i=i),
                    **params
                ) >> end_dummy
            return start_dummy, end_dummy

    @staticmethod
    def build_params(config):
        params = {}
        # flag
        try:
            params[FLAG] = config[FLAG]
        except KeyError as e:
            pass

        # recursive
        try:
            params[RECURSIVE] = config[RECURSIVE]
        except KeyError as e:
            pass

        # min_size
        try:
            params[MIN_SIZE] = config[MIN_SIZE]
        except KeyError as e:
            pass
        return params

    @staticmethod
    def build_params_decorator(func):
        def wrap_func(config):
            params = HdfsSensorTaskFactory.build_params(config)
            params.update(func(config))
            return params

        return wrap_func


class BashSensorTaskFactory(AbstractTaskFactory):
    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[BASH_SENSOR_TASK_TYPE] = BashSensorTaskFactory.create_task
            return func(self)

        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        command_template = " ".join(de_config[JsonConfigReader.get_property(task_config, COMMAND_TEMPLATE)])
        command_params = JsonConfigReader.get_property(task_config, "{options}.{command_params}"
                                                       .format(options=OPTIONS, command_params=COMMAND_PARAMS))
        command = command_template.format(**command_params)
        print(command)
        task = BashSensor(
            bash_command=command,
            task_id="{name}".format(name=JsonConfigReader.get_property(task_config, NAME)),
            dag=dag, **params)
        return task, task


class GCSSensorTaskFactory(AbstractTaskFactory):
    @staticmethod
    def intercept_task(func):
        def wrap_func(self):
            self.tasks[GCS_SENSOR_TASK_TYPE] = GCSSensorTaskFactory.create_task
            return func(self)

        return wrap_func

    @classmethod
    def create_task(cls, dag, task_config, params):
        params = {
            "bucket": JsonConfigReader.get_property(task_config, BUCKET),
            "object": JsonConfigReader.get_property(task_config, OBJECT)
        }

        if(JsonConfigReader.get_property(task_config, GOOGLE_CLOUD_CONN_ID)):
            params["google_cloud_conn_id"] = JsonConfigReader.get_property(task_config, GOOGLE_CLOUD_CONN_ID)

        if(JsonConfigReader.get_property(task_config, DELEGATE_TO)):
            params["delegate_to"] = JsonConfigReader.get_property(task_config, DELEGATE_TO)

        from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
        task = GoogleCloudStorageObjectSensor(dag=dag, task_id="{name}".format(name=JsonConfigReader.get_property(task_config, NAME)), **params)
        return task, task
