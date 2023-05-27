from Connection import Connect
from datetime import datetime
import json
class action_options():
    def __init__(self, id=None, action_id=None, exprs=None, partitions=None, other_dataset=None, cols=None, join_type=None, id_rename=None):
        self.id = id
        self.action_id = action_id
        self.exprs= exprs
        self.partitions= partitions
        self.other_dataset= other_dataset
        self.cols= cols
        self.join_type= join_type
        self.id_rename= id_rename

    def InsertAction_option(self, action_options):
        conn = Connect()
        insert_action_option = """ insert into option_actions(action_id, exprs, partitions, other_dataset, cols, join_type, id_rename) values (%s, %s, %s, %s, %s, %s, %s)"""
        parame_name = (action_options.action_id, action_options.exprs, action_options.partitions, action_options.other_dataset, action_options.cols, action_options.join_type,
                       action_options.id_rename,)
        conn.Insert_item(insert_action_option,parame_name)

    def Delete_Action_option(self, action_id):
        conn = Connect()
        for id in action_id:
            delete_etl = """DELETE FROM option_actions WHERE action_id = %s"""
            id_etl = (id,)
            conn.Insert_item(delete_etl, id_etl)
