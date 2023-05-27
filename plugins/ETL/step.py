from Connection import Connect
from datetime import datetime
from ETL.etl import ETL
from ETL.action_options import action_options
from ETL.action import action
from ETL.input_options import option_input
from ETL.output_options import option_output

import json
import os
import re

class Step():
    def __init__(self, id=None,etl_id=None, name=None, input_format=None, output_format=None):
        self.id = id
        self.etl_id = etl_id
        self.name = name
        self.input_format= input_format
        self.output_format = output_format

    def InsertStep(self, Step):
        conn = Connect()
        insert_etl = """ insert into Step(etl_id, name, input_format, output_format) values (%s, %s,%s, %s)"""
        parame_name = (Step.etl_id, Step.name, Step.input_format, Step.output_format)
        conn.Insert_item(insert_etl,parame_name)

    def LatestID(self, name):
        conn = Connect()
        selectID_action = """select id from Step where name = %s order by id desc limit 1"""
        parame_name = (name, )
        data = conn.Select_item(selectID_action, parame_name)
        return data
    
    def get_Step_id(self, name):
        conn = Connect()
        sql_id = """select id from step where name = %s"""
        para = (name,)
        data = conn.Select_item(sql_id, para)
        return data
    
    def get_Name_Step(self, id):
        conn = Connect()
        sql_name = """select name from step where id = %s"""
        para = (id,)
        data = conn.Select_item(sql_name, para)
        return data
    
    def get_Step_id_etl_id(self, id_etl):
        conn = Connect()
        sql_id_etl = """select etl_id from step where id = %s"""
        para = (id_etl,)
        id = conn.Select_item(sql_id_etl, para)
        return id

    def ListStep(self, id_etl):
        conn = Connect()
        lst_etl = []
        selectETL = """select id, name, input_format, output_format from step where etl_id= %s"""
        value_name = (id_etl,)
        data = conn.SelectAll_item(selectETL, value_name)
        for s in data:
            s = Step(s[0],None, s[1], s[2], s[3])
            lst_etl.append(s)
        return lst_etl
    
    def select_Step(self, id):
        conn = Connect()
        lst=[]
        sql_name = """select name from step where etl_id = %s"""
        para = (id,)
        Alloption = conn.SelectAll_item(sql_name, para)

        for n in Alloption:
            lst_name = list(n)
            for name_etl in lst_name:
                lst.append(name_etl)
        return lst
    
    def action_option_id(self, step_id):
        conn = Connect()

        sql_other_dateset = """select id from actions where step_id = %s"""
        para = (step_id,)
        All_other_dataset = conn.SelectAll_item(sql_other_dateset, para)

        lst = []
        
        for item in list(All_other_dataset):
            item = list(item)
            for i in item:
                lst.append(i)
        return lst
    def Delete_Step(self, id):
        ac_op = action_options()
        ac = action()
        input_ac = option_input()
        output_ac = option_output()

        ac_op.Delete_Action_option(self.action_option_id(id))
        ac.Delete_Action(id)
        input_ac.Delete_Option_Input(id)
        output_ac.Delete_Option_Output(id)

        conn = Connect()
        delete_step = """DELETE FROM Step WHERE id = %s"""
        id_step = (id,)
        conn.Insert_item(delete_step, id_step)

    def json_input_step(self, step_id):
        conn = Connect()

        sql_input_format = """select input_format from Step where id = %s"""
        para = (step_id,)
        input_format = conn.Select_item(sql_input_format, para)

        sql_streaming = """select streaming from options_input where step_id = %s"""
        para = (step_id,)
        Alloption = conn.Select_item(sql_streaming, para)

        dic={'streaming':Alloption}
        dic = {k: v for k, v in dic.items() if v is not None}
           
        keys =['format', 'options']
        lst_value=[]
        
        lst_value.append(input_format)
        lst_value.append(dic)
        res = dict(zip(keys, lst_value))
        self.clearDic(res)
        return res
    
    def json_output(self, step_id):
        my_dictionary = None
        conn = Connect()
        # select input format
        sql_output_format = """select output_format from Step where id = %s"""
        para = (step_id,)
        output_format = conn.Select_item(sql_output_format, para)

        #select list all option of input
        lst_options = []
        sql_all_options_output = """select path, output_mode, partition_cols from options_output where step_id = %s"""
        para = (step_id,)
        Alloption = conn.SelectAll_item(sql_all_options_output, para)

        for argument in Alloption:
            lst_options.append(argument)

        key=['path', 'output_mode', 'partition_cols']
        for item in lst_options:
            lst_2 = list(item)
            for i in lst_2:
                my_dictionary = dict(zip(key, lst_2))
                self.clearDic(my_dictionary)
        
        keys =['format', 'options']
        lst_value=[]
        lst_value.append(output_format)
        lst_value.append(my_dictionary)
        res = dict(zip(keys, lst_value))
        self.clearDic(res)
        return res

    def json_option(self, id):
        conn = Connect()

        sql_options_action = """select exprs, cols from option_actions where action_id = %s"""
        para = (id,)
        Alloption = conn.SelectAll_item(sql_options_action, para)
        return Alloption
    
    def sek(self, id):
        conn = Connect()

        sql_other_dateset = """select other_dataset from option_actions where action_id = %s"""
        para = (id,)
        All_other_dataset = conn.Select_item(sql_other_dateset, para)

        return All_other_dataset
    

    
    def Handle_option_UNION(self, id):
        di ={}
        conn = Connect()

        sql_other_dateset = """select other_dataset from option_actions where action_id = %s"""
        para = (id,)
        All_other_dataset = conn.Select_item(sql_other_dateset, para)
        di['OTHER_DATASETS'] = All_other_dataset
        return di
    
    def Handle_option_REPARTITION(self, id):
        conn = Connect()

        sql_partitions = """select partitions from option_actions where action_id = %s"""
        para = (id,)
        All_partitions = conn.Select_item(sql_partitions, para)

        di ={}
        di['PARTITIONS'] = All_partitions
        return di
    
    def Handle_option_JOIN(self, id):
        my_dictionary ={}
        conn = Connect()

        sql_option_join = """select join_type, other_dataset from option_actions where action_id = %s"""
        para = (id,)
        All_option_join = conn.SelectAll_item(sql_option_join, para)

        key = ['JOIN_TYPE', 'OTHER_DATASETS']
        lst_options=[]
        for argument in All_option_join:
            lst_options.append(argument)

        for item in lst_options:
            lst_2 = list(item)
            for i in lst_2:
                my_dictionary = dict(zip(key, lst_2))
                self.clearDic(my_dictionary)
        return my_dictionary
    
    def get_Exprs(self, id):
        conn = Connect()

        sql_exprs = """select exprs from option_actions where action_id = %s"""
        para = (id,)
        All_other_dataset = conn.Select_item(sql_exprs, para)

        return All_other_dataset
    
    def Handle_option_RENAME(self, id):
        conn = Connect()

        sql_option_rename = """select id_rename from option_actions where action_id = %s"""
        para = (id,)
        All_option_rename = conn.Select_item(sql_option_rename, para)

        di ={}
        di['id'] = All_option_rename
        return di

    def json_action(self, step_id):
        conn = Connect()
        lst_res = []
        res = []
        my_dictionary ={}
        
        sql_actions = """select id, name from actions where step_id = %s"""
        para = (step_id,)
        list_id_action = conn.SelectAll_item(sql_actions, para)

        for item in list_id_action:
            lst_res = []
            lst_id_name = list(item)

            lst_res.append(lst_id_name[1])
            
            if (lst_id_name[1] == 'union'):
                keys=['plan', 'exprs', 'options']
                lst_res.append(self.Handle_option_UNION(item[0]))
                my_dictionary = dict(zip(keys, lst_res))
                my_dictionary=self.clearDic(my_dictionary)
                js = json.dumps(my_dictionary)
            elif(lst_id_name[1] == 'repartition'):
                keys=['plan','options']
                lst_res.append(self.Handle_option_REPARTITION(item[0]))
                my_dictionary = dict(zip(keys, lst_res))
                my_dictionary=self.clearDic(my_dictionary)
                js = json.dumps(my_dictionary)
            elif (lst_id_name[1] == 'join'):
                keys=['plan', 'exprs','options']
                lst_res.append(self.get_Exprs(item[0]))
                lst_res.append(self.Handle_option_JOIN(item[0]))
                my_dictionary = dict(zip(keys, lst_res))
                my_dictionary=self.clearDic(my_dictionary)
                js = json.dumps(my_dictionary)
            elif (lst_id_name[1] == 'rename'):
                keys=['plan','options']
                lst_res.append(self.Handle_option_RENAME(item[0]))
                my_dictionary = dict(zip(keys, lst_res))
                my_dictionary=self.clearDic(my_dictionary)
                js = json.dumps(my_dictionary)
            else :
                keys = ['plan', 'exprs', 'cols']
                lst_option = self.json_option(item[0])
                for item in lst_option:
                    list_tup = list(item)
                    flat_list = [item for sublist in list_tup for item in sublist]
                    exprs = flat_list[0]
                    cols = flat_list[1]
                    break
                # print(type(s))
                # for i in s:
                #     cols = i
                if exprs is None:
                    keys = ['plan', 'cols']
                    lst_res.append(cols)
                elif cols is None:
                    keys = ['plan', 'exprs']
                    lst_res.append(exprs)
                else:
                    # for item in lst_option:
                    #     item = list(item)
                    #     for i in item:
                    #         lst_res.append(i)
                    lst_res.append(exprs)
                    lst_res.append(cols)
                my_dictionary = dict(zip(keys, lst_res))
                my_dictionary=self.clearDic(my_dictionary)
                my_dictionary = {x:y for x,y in my_dictionary.items() if y is not None}
                js = json.dumps(my_dictionary)
            res.append(js)
        return res
    
    def parse_conf_step(self, id_step):
        dict_res={}
        dict_res['input']=self.json_input_step(id_step)
        lst = []
        for dict in self.json_action(id_step):
            res=json.loads(dict)
            lst.append(res)
        dict_res['actions'] = lst
        dict_res['output']=self.json_output(id_step)
        self.clearDic(dict_res)

        dict_res_final = {self.get_Name_Step(id_step):dict_res}
        return json.dumps(dict_res_final)
    
    def apply(self, id_etl):
        e = ETL()
        conn = Connect()
        sql_id_step = """select id from Step where etl_id = %s"""
        para = (id_etl,)
        id_step = conn.SelectAll_item(sql_id_step, para)
        lst = []
        lst_dict =[]

        for item in id_step:
            step = list(item)
            for s in step:
                dic = self.parse_conf_step(s)
                lst.append(dic)

        for d in lst:
            res = json.loads(d)
            lst_dict.append(res)

        dict_res_final={"com.nineoneonedata.spark":lst_dict}

        # Dict = {'com.nineoneonedata.spark':}
        filename = e.getFileName(e.get_Name(id_etl))
        with open(filename, 'w') as convert_file:
                convert_file.write(json.dumps(dict_res_final))
    def clearDic(self, my_dict):
        for key, value in list(my_dict.items()):
            if not value :
                del my_dict[key]
        return my_dict
    
    def transform(self,nested_list):
        regular_list = []
        for ele in nested_list:
            if type(ele) is list:
                regular_list.append(ele)
            else:
                regular_list.append([ele])
        return regular_list