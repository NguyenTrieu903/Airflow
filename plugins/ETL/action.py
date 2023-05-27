from Connection import Connect
from datetime import datetime
import json
class action():
    lst_action = ['transform', 'fillter','select','union','dedup','repartition','join','rename']
    def __init__(self, id=None,step_id=None, name=None,exprs=None, partitions=None, other_dataset=None, 
                cols=None, join_type = None, id_rename =None):
        self.id = id
        self.step_id = step_id
        self.name= name
        self.exprs= exprs
        self.partitions= partitions
        self.other_dataset= other_dataset
        self.cols= cols
        self.join_type= join_type
        self.id_rename= id_rename
        

    def InsertAction(self, action):
        conn = Connect()
        insert_action = """ insert into actions(step_id, name) values (%s, %s)"""
        parame_name = (action.step_id, action.name)
        conn.Insert_item(insert_action,parame_name)

    def LatestID(self, name):
        conn = Connect()
        selectID_action = """select id from actions where name = %s order by id desc limit 1"""
        parame_name = (name, )
        data = conn.Select_item(selectID_action, parame_name)
        return data
    
    def ListAction(self, step_id):
        conn = Connect()
        lst_etl = []
        sql_select = """select a.id, a.name, op.exprs, op.partitions, op.other_dataset, op.cols, op.join_type, op.id_rename from actions a
                        join option_actions op on a.id=op.action_id 
                        where a.step_id= %s"""
        para = (step_id,)
        data = conn.SelectAll_item(sql_select, para)
        for a in data:
            s = action(a[0],None, a[1],a[2],a[3],a[4],a[5], a[6],a[7])
            lst_etl.append(s)
        return lst_etl
    
    def Delete_Action(self, step_id):
        conn = Connect()
        delete_etl = """DELETE FROM actions WHERE step_id = %s"""
        id_etl = (step_id,) 
        conn.Insert_item(delete_etl, id_etl)
    
    
    

    