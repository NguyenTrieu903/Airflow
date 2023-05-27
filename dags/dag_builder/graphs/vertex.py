class Vertex:
    def __init__(self, name, payload):
        self.name = name
        self.up_streams = []
        self.down_streams = []
        self.payload = payload

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return self.name

    def add_up_stream(self, up_stream):
        self.up_streams.append(up_stream)

    def add_down_stream(self, down_stream):
        self.down_streams.append(down_stream)
