from Connection import Connect
from datetime import datetime
import json
import os
import re

class Dag():

    def __init__(self, id=None, name=None):
        self.id = id
        self.name = name

    def PrintDag(self):
        print("Dag Id:", self.id, "Dag Name: ",self.name)
    # conn = Connect()
    def ListDag(self):
        conn = Connect()
        selectTaskname = """select * from ScheduleDag"""
        data = conn.SelectAll_item(selectTaskname)
        return data

    def SelectDag(self, id):
        conn = Connect()
        selectDag = """select * from ScheduleDag where id = %s"""
        value_id = (id,)
        data = conn.SelectAll_item(selectDag, value_id)
        return data

    def UpdateDag(self, Dag):
        conn = Connect()

        query_dagName = """select name from ScheduleDag where id = %s"""
        value_name = (Dag.id,)
        dag_name = conn.Select_item(query_dagName, value_name)

        query_ConfigName = """select config_name from dag_file_mapping where dag_id = %s"""
        value_name = (Dag.id,)
        ConfigName_name = conn.Select_item(query_ConfigName, value_name)
        
        updateDag = """update ScheduleDag set name = %s where id = %s"""
        value = (Dag.name, Dag.id)
        conn.Insert_item(updateDag, value)

        date, date1 = self.getDate()
        updateDag = """update dag_file_mapping set config_name = %s where dag_id = %s"""
        config_file = "config_{dag_name}_{date}.json".format(dag_name=Dag.name, date=date)
        value = (config_file, Dag.id)
        conn.Insert_item(updateDag, value)

        
        file_py = self.getFileNamePy(dag_name)
        file_py = "dags/{file_py}".format(file_py=file_py)
        dag_file = "dags/{dag_name}_{date1}.py".format(dag_name=Dag.name, date1=date1)

        file_json = self.getFileNameJson(dag_name)
        file_json = "dags/json/{file_json}".format(file_json=file_json)
        json_file = "dags/json/{dag_name}_{date}.json".format(dag_name=Dag.name, date=date)

        file_config = "dags/configure_bash/{ConfigName_name}".format(ConfigName_name=ConfigName_name)
        config_file = "dags/configure_bash/config_{dag_name}_{date}.json".format(dag_name=Dag.name, date=date)
        
        os.rename(file_py, dag_file)
        os.rename(file_json, json_file)
        os.rename(file_config, config_file)
    
    def getOldFile(self, oldName):
        old_py = self.getFileNamePy(oldName)
        old_json = self.getFileNameJson(oldName)
        return old_py, old_json
    
    def changeFile(self, newName, oldName,old_py,old_json):
        date, date1 = self.getDate()

        new_name = self.getFileNamePy(newName)
        new_json = self.getFileNameJson(newName)
        newname = new_name.replace(".py","")
        old_py = old_py.replace(".py","")

        path_new_name = "dags/{newName}".format(newName=new_name)
        path_old_json = "dags/json/{olname}".format(olname=old_json)
        fin = open(path_new_name, "rt")
        data = fin.read()

        path_json_new = "dags/json/{new_file}".format(new_file=new_json)

        data = data.replace(path_old_json, path_json_new)
        data = data.replace(old_py, newname)

        fin.close()
        path_abs =  "dags/{dag_file}_{date1}.py".format(dag_file=newName, date1=date1)
        
        with open(path_abs, "w") as fp2:
            fp2.write(data)

    def InsertDag(self, Dag):
        conn = Connect()
        insert_dag = """ insert into ScheduleDag(name) values (%s)"""
        parame_name = (Dag.name, )
        conn.Insert_item(insert_dag,parame_name)

    def SelectAllTask(self, id):
        conn = Connect()

        selectTaskname = """select name from task where dag_id = %s"""
        value_id = (id,)
        data = conn.SelectAll_item(selectTaskname, value_id)
        data = list(data)
        return data
    def getDate(self):
        date = datetime.today().strftime('%Y-%m-%d%H:%M:%S')
        date = date.replace('-','')
        date = date.translate({ord(':'): None})

        date1 = datetime.today().strftime('%Y-%m-%d')
        date1 = date1.replace('-','')

        return date, date1
    def DeleteDag(self, id):
        conn = Connect()

        query_ConfigName = """select config_name from dag_file_mapping where dag_id = %s"""
        value_name = (id,)
        ConfigName_name = conn.Select_item(query_ConfigName, value_name)

        delete_config = """DELETE FROM dag_file_mapping WHERE dag_id = %s"""
        value_id = (id,)
        conn.Insert_item(delete_config, value_id)

        query_dagName = """select name from ScheduleDag where id = %s"""
        value_name = (id,)
        dag_name = conn.Select_item(query_dagName, value_name)

        delete = """DELETE FROM ScheduleDag WHERE id = %s"""
        value_id = (id,)
        conn.Insert_item(delete, value_id)

        file_py = self.getFileNamePy(dag_name)
        file_json = self.getFileNameJson(dag_name)
        file_config = ConfigName_name
        path_py = 'dags/{file_name}'.format(file_name=file_py)
        path_json = 'dags/json/{file_name}'.format(file_name=file_json)
        path_config = 'dags/configure_bash/{file_name}'.format(file_name=file_config)
        os.remove(path_py)
        os.remove(path_json)
        os.remove(path_config)

    def createFile(self, dag_name):
        date, date1 = self.getDate()
        json_file = "{dag_name}_{date}.json".format(dag_name=dag_name, date=date)
        
        dag_file = "{dag_name}_{date1}.py".format(dag_name=dag_name, date1=date1)

        fin = open("template_dag/templates.py", "rt")
        #read file contents to string
        data = fin.read()
        #replace all occurrences of the required string
        path_json_file = 'dags/json/{json_file}'.format(json_file=json_file)
        data = data.replace('/path/to/to/dagName_YYYYmmddHH.json', path_json_file)
        dag_file1 = "{dag_name}_{date1}".format(dag_name=dag_name, date1=date1)
        data = data.replace('dagName_YYYYmmdd', dag_file1)
        
        #close the input file
        fin.close()
        path_abs =  "dags/{dag_file}".format(dag_file=dag_file)
        
        with open(path_abs, "w") as fp2:
            fp2.write(data)
        path_abs_file_json =  "dags/json/{json_file}".format(json_file=json_file)
        f1 = open(path_abs_file_json, "w")

    def saveFile(self, dag_name):
        conn = Connect()

        query_dagid = """select id from ScheduleDag where name = %s"""
        value_name = (dag_name,)
        dag_id = conn.Select_item(query_dagid, value_name)

        dag_file_mapping = """ insert into dag_file_mapping(dag_id, config_name) values (%s, %s)"""

        
        date = datetime.today().strftime('%Y-%m-%d%H:%M:%S')
        date = date.replace('-','')
        date = date.translate({ord(':'): None})

        config_name = "config_{dag_name}_{date}.json".format(dag_name=dag_name, date=date)
        parame_name = (dag_id, config_name, )

        conn.Insert_item(dag_file_mapping,parame_name)
        path_abs_file_config = "dags/configure_bash/{config_file}".format(config_file=config_name)
        f1 = open(path_abs_file_config, "w")

    def select_configFile(self, dag_id):
        conn = Connect()
        selectconfig_name = """select config_name from dag_file_mapping where dag_id = %s"""
        value_name = (dag_id,)
        config_name = conn.Select_item(selectconfig_name, value_name)
        return config_name

    def getFileName(self, filename):
        path = "dags/json"
        dir_list = os.listdir(path)
        for file in dir_list:
            if filename in file:
                return file
            
    def getFileNamePy(self, filename):
        src = "{dag_name}_[0-9]+.py".format(dag_name=filename)
        path = "dags"
        dir_list = os.listdir(path)
        for file in dir_list:
            if bool(re.match(src, file)):
                return file
            
    def getFileNameJson(self, filename):
        src = "{dag_name}_[0-9]+.json".format(dag_name=filename)
        path = "dags/json"
        dir_list = os.listdir(path)
        for file in dir_list:
            if bool(re.match(src, file)):
                return file
            
    def getFileNameConfigure(self, filename):
        src = "config_{dag_name}_[0-9]+.json".format(dag_name=filename)
        path = "dags/configure_bash"
        dir_list = os.listdir(path)
        for file in dir_list:
            if bool(re.match(src, file)):
                return file

    def bash_config(self, id_dag):
        conn = Connect()
         
        # selectconfig_name = """select name from dag where id = %s"""
        # value_name = (id_dag,)
        # config_name = conn.Select_item(selectconfig_name, value_name)

        selectCommand = """select command_template from task where dag_id = %s and task_type = %s or task_type = %s"""
        value_dagID = (id_dag,'bash','bash-sensor')
        command_tempaltes = conn.SelectAll_item(selectCommand, value_dagID)
        # print(command_tempaltes)
        lst = []
        for item in list(command_tempaltes):
            item = list(item)
            for i in item:
                lst.append(i)
        # return lst
        # for item in command_tempaltes:
        #     list_tup = list(item)
        #     flat_list = [item for sublist in list_tup for item in sublist]
        # lst_command = list(command_tempaltes)
        # for commad in lst_command:
        #     lst = list(commad)
        dict = {}
        value= "A_bash_command_or_script  /path/on_hdfs/or_on_file_system/upto_date={date}".format(date=datetime.today().strftime('%Y-%m-%d'))
        lis_val = [value]
        for bash in command_tempaltes:
            str1 = ''.join(bash)
            dict[str1] = lis_val
        file = self.select_configFile(id_dag)
        file = "dags/configure_bash/{file}".format(file=file)
        print(file)
        # f = open(file, "w")
        dict = json.dumps(dict)
        with open(file, "w") as fp2:
            # dict = json.dumps(dict)
            fp2.write(dict)
        # f.write(dict)
        # f.close()