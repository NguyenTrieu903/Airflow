from airflow.operators.dummy_operator import DummyOperator

from dag_builder.tasks.dummy_task_factory import DummyTaskFactory

from dag_builder.builder.json_config_reader import JsonConfigReader

from dag_builder.builder.flow_builder import FlowBuilder


class JsonFlowBuilder(FlowBuilder):
    def __init__(self, dag, flow_file_name, task_id):
        self.dag = dag
        self.flow_file_name = flow_file_name
        self.tasks = {}
        self.tasks_config = None
        self.task_id = task_id

    def get_config(self):
        self.tasks_config = JsonConfigReader.read(self.flow_file_name)

    def create_tasks(self):
        for t in self.tasks_config["tasks"]:
            self.tasks[t["name"]] = DummyTaskFactory.create_task(dag=self.dag, task_config=t)

    def create_upstreams(self):
        for t in self.tasks_config["tasks"]:
            if t["upstreams"]:
                for tt in t["upstreams"]:
                    self.tasks[tt][1] >> self.tasks[t["name"]][0]

    def create_start(self):
        start_dummy = DummyOperator(dag=self.dag, task_id="start-{task_id}".format(task_id=self.task_id))
        for t in self.tasks_config["tasks"]:
            if not t["upstreams"]:
                start_dummy >> self.tasks[t["name"]][0]
        return start_dummy

    def create_end(self):
        end_dummy = DummyOperator(dag=self.dag, task_id="end-{task_id}".format(task_id=self.task_id))
        for t in self.tasks_config["tasks"]:
            status = True
            for tt in self.tasks_config["tasks"]:
                if t["name"] in tt["upstreams"]:
                    status = False
                    break
            if status:
                self.tasks[t["name"]][1] >> end_dummy
        return end_dummy