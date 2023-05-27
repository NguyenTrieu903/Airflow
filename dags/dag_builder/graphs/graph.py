from queue import Queue

from dag_builder.common.constant import TASKS, NAME, UPSTREAMS

from dag_builder.graphs.vertex import Vertex


class Graph:
    def __init__(self, config):
        self.config = config
        self.vertices = {}

    def build(self):
        self.create_vertices()
        self.create_edges()

    def create_vertices(self):
        for config in self.config[TASKS]:
            self.create_vertex(config=config)

    def create_vertex(self, config):
        self.vertices[config[NAME]] = Vertex(name=config[NAME], payload=config)

    def create_edges(self):
        for config in self.config[TASKS]:
            for u in config[UPSTREAMS]:
                self.vertices[config[NAME]].add_up_stream(self.vertices[u])
                self.vertices[u].add_down_stream(self.vertices[config[NAME]])

    def reverse_edge(self, start, end):
        end_node = None
        for d in self.vertices[start].down_streams:
            if d.name == end:
                end_node = d
                break
        self.vertices[start].down_streams.remove(end_node)
        self.vertices[start].up_streams.append(end_node)
        start_node = None
        for d in self.vertices[end].up_streams:
            if d.name == start:
                start_node = d
                break
        self.vertices[end].up_streams.remove(start_node)
        self.vertices[end].down_streams.append(start_node)

    def exist_circle(self):
        path = {}
        visited = {}
        for i in self.vertices:
            path[i] = 0
            visited[i] = 0
        vertices_stack = []

        def rec_func():
            vertex = vertices_stack.pop()
            path[vertex.name] = 1
            visited[vertex.name] = 1
            for d in vertex.down_streams:
                if visited[d.name] == 0:
                    vertices_stack.append(d)
                    status = rec_func()
                    if status:
                        return status
                elif visited[d.name] == 1:
                    if path[d.name] == 1:
                        return True
            path[vertex.name] = 0

        while Graph.exist_not_visited(visited):
            vertices_stack.append(self.vertices[Graph.get_not_visited(visited)])
            s = rec_func()
            if s:
                return True
        return False

    def dfs(self, func):
        visited = {}
        vertices_stack = []
        for i in self.vertices:
            visited[i] = 0

        def rec_func():
            vertex = vertices_stack.pop()
            if visited[vertex.name] == 0:
                func(vertex)
                visited[vertex.name] = 1
            for d in vertex.down_streams:
                if visited[d.name] == 0:
                    vertices_stack.append(d)
                    rec_func()

        while Graph.exist_not_visited(visited):
            vertices_stack.append(self.vertices[Graph.get_not_visited(visited)])
            rec_func()

    @staticmethod
    def exist_not_visited(visited):
        return list(filter(lambda x: visited[x] == 0, visited))

    @staticmethod
    def get_not_visited(visited):
        if Graph.exist_not_visited(visited):
            return list(filter(lambda x: visited[x] == 0, visited))[0]
        return None

    def bfs(self, func):
        visited = {}
        for i in self.vertices:
            visited[i] = 0

        vertices_queue = Queue()

        while not vertices_queue.empty() or Graph.exist_not_visited(visited):
            if vertices_queue.empty():
                vertices_queue.put(self.vertices[Graph.get_not_visited(visited)])
            vertex = vertices_queue.get()
            if visited[vertex.name] == 0:
                func(vertex)
                visited[vertex.name] = 1
                for d in vertex.down_streams:
                    if visited[d.name] == 0:
                        vertices_queue.put(d)

    def print_graph(self):
        for v in self.vertices:
            print("=======Vertex ({name})=======".format(name=self.vertices[v].name))
            print("up streams:")
            for u in self.vertices[v].up_streams:
                print(u.name)
            print("down streams:")
            for d in self.vertices[v].down_streams:
                print(d.name)

    def get_edges(self):
        edges = []
        for v in self.vertices:
            for d in self.vertices[v].down_streams:
                edges.append((v, d.name))
        return edges

    def remove_edge(self, start, end):
        end_node = None
        for d in self.vertices[start].down_streams:
            if end == d.name:
                end_node = d
                break
        start_node = None
        for u in self.vertices[end].up_streams:
            if start == u.name:
                start_node = u
                break
        start_node.down_streams.remove(end_node)
        end_node.up_streams.remove(start_node)

    def transitive_reduce(self):
        edges = self.get_edges()
        for edge in edges:
            self.reverse_edge(start=edge[0], end=edge[1])
            status = self.exist_circle()
            self.reverse_edge(start=edge[1], end=edge[0])
            if status:
                self.remove_edge(edge[0], edge[1])
