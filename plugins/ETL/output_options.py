from Connection import Connect
from datetime import datetime
import json

class option_output():
    def __init__(self, id=None,step_id=None, path=None,output_mode=None, partition_cols=None):
        self.id=id
        self.step_id = step_id
        self.path = path
        self.output_mode = output_mode
        self.partition_cols = partition_cols
    
    def InsertOptionOutput(self, option_output):
        conn = Connect()
        insert_output = """ insert into options_output(step_id, path, output_mode, partition_cols) values (%s, %s, %s, %s)"""
        parame_name = (option_output.step_id, option_output.path, option_output.output_mode, option_output.partition_cols)
        conn.Insert_item(insert_output,parame_name)

    def Delete_Option_Output(self, step_id):
        conn = Connect()
        delete_etl = """DELETE FROM options_input WHERE step_id = %s"""
        id_etl = (step_id,)
        conn.Insert_item(delete_etl, id_etl)