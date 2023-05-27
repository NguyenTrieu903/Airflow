from airflow.operators.dummy_operator import DummyOperator

from dag_builder.graphs.graph import Graph

from dag_builder.builder.json_config_reader import JsonConfigReader

from dag_builder.builder.flow_builder import FlowBuilder


class GraphFlowBuilder(FlowBuilder):
    def __init__(self, dag, flow_file_name, task_id):
        self.dag = dag
        self.flow_file_name = flow_file_name
        self.task_id = task_id
        self.graph = None
        self.tasks = {}

    def create_tasks(self):
        def create_task(vertex):
            from dag_builder.tasks.utils.task_factory import TaskFactory
            task_factory = TaskFactory()
            self.tasks[vertex.name] = task_factory.create_task(dag=self.dag, task_config=vertex.payload)
        self.graph.dfs(func=create_task)

    def create_upstreams(self):
        def create_upstream(vertex):
            for u in vertex.up_streams:
                self.tasks[vertex.name][0] << self.tasks[u.name][1]
        self.graph.dfs(func=create_upstream)

    def create_start(self):
        start_dummy = DummyOperator(dag=self.dag, task_id="start-{task_id}".format(task_id=self.task_id))
        no_up_streams = []

        def has_up_stream(vertex):
            if not vertex.up_streams:
                no_up_streams.append(vertex)
        self.graph.dfs(has_up_stream)
        for i in no_up_streams:
            start_dummy >> self.tasks[i.name][0]
        return start_dummy

    def create_end(self):
        end_dummy = DummyOperator(dag=self.dag, task_id="end-{task_id}".format(task_id=self.task_id))
        no_down_streams = []

        def has_down_stream(vertex):
            if not vertex.down_streams:
                no_down_streams.append(vertex)
        self.graph.dfs(has_down_stream)
        for i in no_down_streams:
            end_dummy << self.tasks[i.name][1]
        return end_dummy

    def get_config(self):
        configs = JsonConfigReader.read(self.flow_file_name)
        self.graph = Graph(configs)
        self.graph.build()
        self.graph.transitive_reduce()

    def optimize_priority_weight(self):
        def optimize(vertex):
            try:
                vertex.payload["priority_weight"] = vertex.payload["duration"]
            except:
                pass
        self.graph.dfs(func=optimize)
