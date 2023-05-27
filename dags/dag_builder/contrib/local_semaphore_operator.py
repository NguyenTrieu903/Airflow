import posix_ipc
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LocalSemaphoreAcquire(BaseOperator):

    template_fields = ["semaphore_name"]
    ui_color = '#ffdc7f'

    @apply_defaults
    def __init__(self, semaphore_name, init_value=1, *args, **kwargs):
        super(LocalSemaphoreAcquire, self).__init__(*args, **kwargs)
        self.semaphore_name = semaphore_name
        self.init_value = init_value

    def execute(self, context):
        semaphore = posix_ipc.Semaphore(self.semaphore_name, flags=posix_ipc.O_CREAT, initial_value=self.init_value)
        try:
            semaphore.acquire()
        except Exception as e:
            e.message
        finally:
            semaphore.close()


class LocalSemaphoreRelease(BaseOperator):

    template_fields = ["semaphore_name"]
    ui_color = '#ffdc7f'

    @apply_defaults
    def __init__(self, semaphore_name, init_value, *args, **kwargs):
        super(LocalSemaphoreRelease, self).__init__(*args, **kwargs)
        self.semaphore_name = semaphore_name
        self.init_value = init_value

    def execute(self, context):
        semaphore = posix_ipc.Semaphore(self.semaphore_name, flags=posix_ipc.O_CREAT, initial_value=0)
        try:
            if semaphore.value < self.init_value:
                semaphore.release()
        except Exception as e:
            e.message
        finally:
            semaphore.close()
