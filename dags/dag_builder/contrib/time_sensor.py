# from airflow.operators.sensors import BaseSensorOperator
# from airflow.sensors import BaseSensorOperator
from airflow.sensors.base import BaseSensorOperator
from dag_builder.contrib.utils import check_time
import datetime

    
class TimeSensor(BaseSensorOperator):
    def __init__(self, on_time, off_time, *args, **kwargs):
        super(TimeSensor, self).__init__(timeout=60 * 60 * 24 * 6, *args, **kwargs)
        self.on_time = on_time
        self.off_time = off_time

    def poke(self, context):
        current_time = datetime.datetime.now().time()
        return check_time(on_time=self.on_time, off_time=self.off_time, current_time=current_time)
