from Connection import Connect
from datetime import datetime
import json
import os
import re

class ETL():
    def __init__(self, id=None, name=None):
        self.id = id
        self.name = name

    def InsertETL(self, ETL):
        conn = Connect()
        insert_etl = """ insert into etl(name) values (%s)"""
        parame_name = (ETL.name,)
        conn.Insert_item(insert_etl,parame_name)

    def ListETL(self):
        conn = Connect()
        lst_etl = []
        selectETL = """select * from etl"""
        data = conn.SelectAll_item(selectETL)
        for etl in data:
            et = ETL(etl[0], etl[1])
            lst_etl.append(et)
        return lst_etl
    
    def deleteEtl(self, id):
        conn = Connect()
        delete_etl = """DELETE FROM ETL WHERE id = %s"""
        id_etl = (id,)
        conn.Insert_item(delete_etl, id_etl)
    
    def get_Step(self, id):
        conn = Connect()
        selectETL = """select * from step where etl_id= %s"""
        value_name = (id,)
        data = conn.SelectAll_item(selectETL, value_name)
        return data

    def get_Name(self, id):
        conn = Connect()
        sql_Name = """select name from ETL where id = %s"""
        para = (id,)
        name = conn.Select_item(sql_Name, para)
        return name
    
    def get_id(self, name):
        conn = Connect()
        sql_id = """select id from etl where name = %s"""
        para = (name,)
        id = conn.Select_item(sql_id, para)
        return id
     
    def createFile(self, id_etl):
        name = self.get_Name(id_etl)
        etl_file = "{etl_name}.conf".format(etl_name=name)
        f = open(etl_file, "w")
        f.close()

    def getFileName(self, name):
        src = "{etl_name}.conf".format(etl_name=name)
        path = "/opt/airflow"
        dir_list = os.listdir(path)
        for file in dir_list:
            if bool(re.match(src, file)):
                return file
    

    



