class AbstractTaskFactory:
    @staticmethod
    def intercept_task(func):
        pass

    @classmethod
    def create_task(cls, dag, task_config, params):
        pass

    @staticmethod
    def build_params(config):
        pass
