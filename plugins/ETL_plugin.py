import imp
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, flash, redirect, url_for
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from ETL.etl import ETL
from ETL.options import *
from ETL.input_options import *
from ETL.action_options import *
from ETL.output_options import *
from ETL.action import *
from ETL.step import * 
from airflow.www.app import csrf
from wtforms import StringField, BooleanField, IntegerField, DateField
from flask_wtf import Form
from wtforms.validators import InputRequired, Length
import json

class ETLAppBuilderBaseView(AppBuilderBaseView):
    default_view = "ListETL"

    @expose("/", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def ListETL(self):
        e = ETL()   
        lst_etl = e.ListETL()
        
        if request.method == 'POST' :
            return redirect('/etlappbuilderbaseview/etl/insert')
        return self.render_template("/ETL/ETL.html", lst_etl=lst_etl)
    
    @expose("/etl/insert", methods=['GET', 'POST'])
    # this method gets the view as localhost:/testappbuilderbaseview/
    @csrf.exempt  # if we don’t want to use csrf
    def insertETL(self):
        if request.method == 'POST':
            name = request.form.get('name')   
            if name == "":
                return redirect(url_for('.ListETL'))
            else:
                et = ETL(None,name)
                et.InsertETL(et)
                et.createFile(et.get_id(et.name))
                return redirect(url_for('.ListETL'))
        return self.render_template("/ETL/insert_ETL.html")
    
    @expose("/etl/delete/<int:id>", methods=['GET', 'POST'])
    # this method gets the view as localhost:/testappbuilderbaseview/
    @csrf.exempt  # if we don’t want to use csrf
    def deleteETL(self, id):
        s = Step()
        e = ETL()
        # lst_step = s.ListStep(id)
        lst_step=e.get_Step(id)

        if not lst_step:
            e.deleteEtl(id)
            return redirect(url_for('.ListETL'))
        else:
            id = id
            return redirect(url_for('.ListStep', id=id))
        
    
    @expose("/step/insert/<int:id>", methods=['GET', 'POST'])
    # this method gets the view as localhost:/testappbuilderbaseview/
    @csrf.exempt  # if we don’t want to use csrf
    def insertStep(self, id):
        st = Step()
        lst_step = st.select_Step(id)
        if request.method == 'POST':
            output_format = None
            output_mode = None
            partition_cols = None
            path = None

            name = request.form.get('name')
            input_format = request.form.get('input_format')
            if name == "":
                return redirect(url_for('.ListStep', id=id))
            else:
                output = request.form.getlist('output')
                streaming = None
                option = request.form.getlist('option')

                if (option):
                    streaming = request.form.get('streaming')
                if(output):
                    output_format = request.form.get('output_format')
                    output_mode = request.form.get('OUTPUT_MODE')
                    partition_cols = request.form.get('PARTITION_COLS')
                    path = request.form.get('path')   

                s = Step(None,id,name, input_format, output_format)
                s.InsertStep(s)
                step_id = s.LatestID(s.name)
                
                op_in = option_input(None,step_id, streaming)
                op_in.InsertOptionInput(op_in)

                op_out = option_output(None, step_id, path, output_mode, partition_cols)
                op_out.InsertOptionOutput(op_out)
                return redirect(url_for('.ListStep', id=id))
        return self.render_template("/ETL/insert_Step.html",lst_name=lst_step)
    
    
    @expose("/ViewStep/<int:id>", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def ListStep(self, id):
        st = Step()   
        lst_step = st.ListStep(id)
        if request.method == 'POST' :
            return redirect(url_for('.insertStep', id=id))
        return self.render_template("/ETL/Step.html", lst_step=lst_step, etl_id=id)
    
    @expose("/Delete/<int:id>", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def DeleteStep(self, id):
        st = Step()   
        print(st.action_option_id(id))
        st.Delete_Step(id)
        return redirect(url_for('.ListStep', id=id))
    
    @expose("/actionStep/insert/<int:id>", methods=['GET', 'POST'])
    # this method gets the view as localhost:/testappbuilderbaseview/
    @csrf.exempt  # if we don’t want to use csrf
    def insertActionStep(self, id):
        s=Step()
        ac = action()
        cols=None
        exprs=None
        orther_dataset=None
        repartition=None
        join_type=None
        id_rename = None
        step_id = id
        
        etl_id=s.get_Step_id_etl_id(id)
        
        if request.method == 'POST':
            lst_check = []
            action_name = request.form.get('action')
            if(action_name=='transform'):
                cols = request.form.get('cols')
                exprs = request.form.get('transform')
                lst_check.extend((cols,exprs))
            elif(action_name=='fillter'):
                exprs = request.form.get('fillter')
                lst_check.append(exprs)
            elif(action_name=='select'):
                exprs = request.form.get('select')
                lst_check.append(exprs)
            elif(action_name=='union'):
                orther_dataset = request.form.get('orther_dataset')
                lst_check.append(orther_dataset)
            elif(action_name=='dedup'):
                cols = request.form.get('cols_dedup')
                lst_check.append(cols)
            elif(action_name=='repartition'):
                repartition = request.form.get('repartition')
                lst_check.append(repartition)
            elif(action_name=='join'):
                exprs = request.form.get('join')
                join_type = request.form.get('join_type')
                orther_dataset = request.form.get('other_dataset_join')
                lst_check.extend((exprs,join_type,orther_dataset))
            elif(action_name=='rename'):
                id_rename = request.form.get('id_rename')
                lst_check.append(id_rename)
            if '' in lst_check:
                return redirect(url_for('.ListStep', id=etl_id))
            else:
                ac = action(None,id, action_name)
                ac.InsertAction(ac)
                if exprs:
                    exprs = exprs.split(',')
                if cols:
                    cols = cols.split(',')
                name = ac.name
                ID_last = ac.LatestID(name)
                op_ac = action_options(None, ID_last, [exprs], repartition, orther_dataset, [cols], join_type, id_rename)
                op_ac.InsertAction_option(op_ac)
            return redirect(url_for('.insertActionStep', id=step_id))
        return self.render_template("/ETL/insert_action.html", etl_id=etl_id, action = ac)
    
    @expose("/ViewAction/<int:id>", methods=['GET', 'POST'])
    # this method gets the view as localhost:/testappbuilderbaseview/
    @csrf.exempt  # if we don’t want to use csrf
    def ListAction(self, id):
        step_id = id
        ac = action()
        lst_action = ac.ListAction(step_id)
        lst_action = list(filter(None, lst_action))
        if request.method == 'POST' :
            return redirect(url_for('.insertActionStep', id=step_id))
        return self.render_template("/ETL/action.html", lst_action=lst_action)
    
    @expose("/action/apply/<int:id>", methods=['GET', 'POST'])
    # this method gets the view as localhost:/testappbuilderbaseview/
    @csrf.exempt  # if we don’t want to use csrf 
    def ApplyAction(self, id):
        s = Step()
        # lst_option=s.json_option(31)
        # lst_options = s.json_action(30)
        # print(lst_option)
        # for item in lst_option:
        #     list_tup = list(item)
        #     flat_list = [item for sublist in list_tup for item in sublist]
        #     print(flat_list)
        #     print(flat_list[0])
        #     print(flat_list[1][0])
        #     break
        s.apply(id)
        return redirect(url_for('.ListETL'))
        