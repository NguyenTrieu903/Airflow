class FlowBuilder:
    def create_flow(self):
        self.get_config()
        self.optimize_priority_weight()
        self.create_tasks()
        self.create_upstreams()
        return self.create_start(), self.create_end()

    def create_tasks(self):
        pass

    def create_upstreams(self):
        pass

    def create_start(self):
        pass

    def create_end(self):
        pass

    def get_config(self):
        pass

    def optimize_priority_weight(self):
        pass
