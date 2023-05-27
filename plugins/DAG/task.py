from Connection import Connect
import json
from enum import Enum
from DAG.option import Option
from datetime import datetime
from DAG.dag import Dag
import os 
import re
class Task():

    Date = Enum("date", ["yyyymmdd", "ddmmyyyy", "mmddyyyy"])
    # Type = Enum("taskType", ["hdfs-sensor", "bash", ""])
    tp = ['hdfs-sensor', 'bash', 'bash-sensor']
    def __init__(self, id=None, name=None, task_type=None, priority_weight=None, pool=None,
                upstreams=None, filepath=None, flag=None, recursive= None, min_size=None, ignore_failed= None, command_template=None, number_of_days=None,
                interval=None, date=None, dataset=None):
        self.id = id
        self.name = name
        self.task_type = task_type
        self.priority_weight = priority_weight
        self.pool = pool
        self.upstreams = upstreams
        self.filepath = filepath
        self.flag = flag
        self.recursive = recursive
        self.min_size = min_size
        self.ignore_failed = ignore_failed
        self.command_template = command_template

        self.number_of_days = number_of_days
        self.interval = interval
        self.date = date
        self.dataset = dataset
        
    def ListTask(self, id):
        conn = Connect()
        lst_task = []
        selectTask = """select * from task where dag_id = %s"""
        value_id = (id,)
        data = conn.SelectAll_item(selectTask, value_id)
        for task in data:
            t = Task(task[0], task[1], task[3], task[4], task[5],task[6],task[7], task[8], task[9], task[10], task[11], task[12])
            lst_task.append(t)
        return lst_task

    def updateTask(self, task):
        conn = Connect()
        if task.task_type == 'hdfs-sensor':
            updateTask = """update task set name = %s, priority_weight=%s, pool=%s,upstreams=%s,filepath=%s,
                        flag=%s, recursive=%s, min_size=%s where id = %s"""
            value = (task.name, task.priority_weight,task.pool,task.upstreams,task.filepath,
                task.flag,task.recursive,task.min_size, task.id)
            conn.Insert_item(updateTask, value)
        elif task.task_type == 'bash':
            updateTask = """update task set name = %s, priority_weight=%s, pool=%s,upstreams=%s,ignore_failed=%s,
                        command_template=%s where id = %s"""
            value = (task.name, task.priority_weight,task.pool,task.upstreams,task.ignore_failed,
                task.command_template, task.id)
            conn.Insert_item(updateTask, value)
        else :
            updateTask = """update task set name = %s, priority_weight=%s, pool=%s,upstreams=%s, command_template=%s where id = %s"""
            value = (task.name, task.priority_weight,task.pool,task.upstreams,task.command_template, task.id)
            conn.Insert_item(updateTask, value)
    
    def convertdate(self,select):
        for d in self.Date:
            if d.value == select:
                date = ""
                result = "".join(dict.fromkeys(d.name))
                l = list(result)
                date = "%"+l[0]+"%"+l[1]+"%"+l[2]
                return date

    def convertTask (self, select):
        return None

    def format_date (self, select):
        for d in self.Date:
            if d.value == select:
                return d.name

    def insertTask(self, upstreams, task, id_dag, format_date, format_previous_date, previous_date):
        conn = Connect()
        insert_task = """ insert into task(name, dag_id, task_type, priority_weight, pool, upstreams, filepath, flag, recursive, min_size, ignore_failed, command_template) values
                     (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        taskname = task.name
        if not upstreams:
            item_tuple_task = (taskname,id_dag,task.task_type,task.priority_weight,task.pool,[],task.filepath,task.flag,
                                task.recursive, task.min_size, task.ignore_failed, task.command_template)
        else :
            item_tuple_task = (taskname,id_dag,task.task_type,task.priority_weight,task.pool,upstreams,task.filepath,task.flag,
                                task.recursive, task.min_size, task.ignore_failed, task.command_template)
        conn.Insert_item(insert_task,item_tuple_task)

        #Options
        query_taskid = "select id from task where name = %s"
        value_name = (taskname,)
        id_task = conn.Select_item(query_taskid, value_name)
        insert_option = """insert into option(task_id, number_of_days, interval, date, format_date, format_previous_date) values
                     (%s, %s, %s, %s, %s,%s)"""

        item_tuple_option = (id_task,task.number_of_days, task.interval, task.date, format_date, format_previous_date)
        conn.Insert_item(insert_option,item_tuple_option)

        #Command
        query_optionid = "select id from option where task_id = %s"
        value_id = (id_task,)
        id_option = conn.Select_item(query_optionid,value_id)

        insert_command = """insert into command(option_id, dataset, date, previous_date) values
                            (%s, %s, %s, %s)"""
        item_tuple_option = (id_option, task.dataset, task.date, previous_date)
        conn.Insert_item(insert_command,item_tuple_option)

    def selecTask(self, id):
        conn = Connect()
        selectDag = """select * from task where id = %s"""
        value_id = (id,)
        data = conn.SelectAll_item(selectDag, value_id)
        for task in data:
            t = Task(task[0], task[1], task[3], task[4], task[5],task[6],task[7], task[8], task[9], task[10], task[11], task[12])
            return t

    def getId_Dag(self, idTask):
        conn = Connect()
        selectidDag = """select dag_id from task where id = %s"""
        value_id = (idTask,)
        data = conn.Select_item(selectidDag, value_id)
        return data

    def get_Upstream(self, id_dag):
        conn = Connect()
        list_taskname =[]
        selectTaskname = """select name from task where dag_id = %s"""
        value_id = (id_dag,)
        data = conn.SelectAll_item(selectTaskname, value_id)
        
        for name in data:
            list_taskname.append(name[0])
        return list_taskname

    def remove_common(self, a, b, name):
        for i in a[:]:
            if i in b:
                a.remove(i)
                # b.remove(i)
        a.remove(name)
        return a
    
    def ListparamTask(self, taskid):
        conn = Connect()
        lst = []
        quey = """select id, name, task_type, priority_weight, pool, upstreams, filepath, flag, recursive, min_size, ignore_failed, command_template from task where dag_id = %s"""
        para = (taskid,)
        Alltask = conn.SelectAll_item(quey, para)
        
        for argument in Alltask:
            lst.append(argument)
        return lst

    def listparamOption(self):
        conn = Connect()
        lst = []
        quey = """select task_id, number_of_days, interval, date from option """
        Alltask = conn.SelectAll_item(quey)

        for argument in Alltask:
            lst.append(argument)
        return lst

    def listparamCommand(self):
        conn = Connect()
        lst = []
        quey = """select option_id, date, previous_date, dataset from command """
        Alltask = conn.SelectAll_item(quey)
        for argument in Alltask:
            lst.append(argument)
        return lst
    
    def handle_option (self, lst_option, taskid):
        keys_op = ['number_of_days', 'interval', 'date']
        for item in lst_option:
            lst_2 = list(item)
            for i in lst_2:
                if i == taskid:
                    lst_2.pop(0)
                    my_dictionary = dict(zip(keys_op, lst_2))
                    return my_dictionary

    def handle_command(self, lst_command, optionid, taskid):
        op = Option()
        type = op.getType(taskid)
        keys_command = ['date', 'previous_date', 'dataset']
        for item in lst_command:
            lst_2 = list(item)
            n = len(lst_2)
            for i in range(0,n):
                if lst_2[i] == optionid and type == "bash":
                    lst_2.pop(0)
                    my_dictionary = dict(zip(keys_command, lst_2))
                    return my_dictionary
                elif  lst_2[i] == optionid and type == "bash-sensor":
                    lst_2.pop(0)
                    keys_command = ['date']
                    my_dictionary = dict(zip(keys_command, lst_2))
                    return my_dictionary

    def handle(self, lst_task, lst_option, lst_command):
        conn = Connect()
        lst_total = []
        keys = ['name', 'task_type',  'priority_weight', 'pool', 'upstreams','filepath', 'flag', 'recursive', 'min_size', 'ignore_failed', 'command_template','options']
        
        for item in lst_task:
            lst_2 = list(item)
            option = self.handle_option(lst_option, item[0])

            query_optionid = "select id from option where task_id = %s"
            value_id = (item[0],)
            id_option = conn.Select_item(query_optionid,value_id)
            
            command = self.handle_command(lst_command, id_option, item[0])

            if command:
                option['command_params'] = command
                option.pop("date",None)
                option = {k: v for k, v in option.items() if v is not None}
            else :
                option = {k: v for k, v in option.items() if v is not None}
                option = {k: v for k, v in option.items() if v != ""}

            lst_2.pop(0)
            lst_2.append(option)
            my_dictionary = dict(zip(keys, lst_2))

            my_dictionary = {k: v for k, v in my_dictionary.items() if v != ""}
            my_dictionary = {k: v for k, v in my_dictionary.items() if v is not None}
            lst_total.append(my_dictionary)
        return lst_total

    def parse_json(self, id_dag):
        conn = Connect()

        query_dagid = """select name from ScheduleDag where id = %s"""
        value_name = (id_dag,)
        dag_name = conn.Select_item(query_dagid, value_name)
        dag = Dag()
        lst_task = self.ListparamTask(id_dag)
        lst_option = self.listparamOption()
        lst_command = self.listparamCommand()
        res = self.handle(lst_task, lst_option, lst_command)

        filename = dag.getFileName(dag_name)
        dict_res = {dag_name:res}
        filename = "dags/json/{name}".format(name = filename)
        with open(filename, 'w') as convert_file:
                convert_file.write(json.dumps(dict_res))

    def deleteTask(self, id):
        #Delete command
        #Get optionid
        conn = Connect()

        selectoptionId = """select id from option where task_id = %s"""
        value_id = (id,)
        optionId = conn.Select_item(selectoptionId, value_id)
        # print(optionId)

        delete_command = """DELETE FROM command WHERE option_id = %s"""
        id_op = (optionId,)
        conn.Insert_item(delete_command, id_op)
        #Delete option
        delete_option = """DELETE FROM option WHERE task_id = %s"""
        id_task_op = (id,)
        conn.Insert_item(delete_option, id_task_op)

        #Delete task
        delete_task = """DELETE FROM task WHERE id = %s"""
        id_task = (id,)
        conn.Insert_item(delete_task, id_task)

        

    