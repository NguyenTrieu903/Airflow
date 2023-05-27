from Connection import Connect

class Options():
    def __init__(self, id=None, streaming=None, path=None, output_mode=None, partition_cols=None):
        self.id = id
        self.streaming = streaming
        self.path = path
        self.output_mode = output_mode
        self.partition_cols = partition_cols

    def insertOptions(self, Options):
        conn = Connect()
        insert_options = """ insert into options(streaming, path, output_mode, partition_cols) values (%s, %s, %s, %s, %s)"""
        parame_name = (Options.streaming, Options.path, Options.output_mode, Options.partition_cols)
        conn.Insert_item(insert_options,parame_name)

