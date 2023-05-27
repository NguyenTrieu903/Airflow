from Connection import Connect
from datetime import datetime
import json

class option_input():
    def __init__(self, id=None, step_id=None, streaming=None):
        self.id = id
        self.step_id = step_id
        self.streaming = streaming
    
    def select_EtlId(self, name):
        conn = Connect()
        selectidEtl = """select id from etl where name = %s"""
        value_name = (name,)
        data = conn.Select_item(selectidEtl, value_name)
        return data


    def InsertOptionInput(self, option_input):
        conn = Connect()
        insert_input = """ insert into options_input(step_id, streaming) values (%s, %s)"""
        parame_name = (option_input.step_id, option_input.streaming)
        conn.Insert_item(insert_input,parame_name)
    
    def Delete_Option_Input(self, step_id):
        conn = Connect()
        delete_etl = """DELETE FROM options_output WHERE step_id = %s"""
        id_etl = (step_id,)
        conn.Insert_item(delete_etl, id_etl)