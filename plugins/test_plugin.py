import imp
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, flash, redirect, url_for
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from DAG.dag import Dag
from DAG.task import Task 
from DAG.option import Option   

from airflow.www.app import csrf
from wtforms import StringField, BooleanField, IntegerField, DateField
from flask_wtf import Form
from wtforms.validators import InputRequired, Length
import json
# from ETL_plugin import ETLAppBuilderBaseView

bp = Blueprint(
    "DAG_plugin",
    __name__,
    # registers airflow/plugins/templates as a Jinja template folder
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/test_plugin",
)

class DAGBuilderBaseView(AppBuilderBaseView):

    default_view = "ListDag"

    @expose("/", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def ListDag(self):
        d = Dag()   
        lst_dag = d.ListDag()
        if request.method == 'POST' :
            return redirect('/dagbuilderbaseview/insertDag')
        return self.render_template("/Dag/dag.html", lst_dag=lst_dag)
    
    @expose("/insertDag", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def insertDag(self):
        
        if request.method == 'POST' :
            name = request.form.get('name')
            if name == "":
                return redirect('/dagbuilderbaseview/')
            else:
                dag = Dag(None,name)

                dag.InsertDag(dag)
                dag.createFile(dag.name)
                dag.saveFile(dag.name)
                return redirect('/dagbuilderbaseview/')
        return self.render_template("/Dag/insertDag.html")

    @expose("/dag/<int:id>/update", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def UpdateDag(self, id):
        d = Dag()
        t = Task()
        dag = d.SelectDag(id)
        for d in dag:
            old_dag = Dag(d[0],d[1])
        if request.method == 'POST' :
            name = request.form.get('name')
            new_dag = Dag(id, name)
            old_py, old_json= new_dag.getOldFile(old_dag.name)
            new_dag.UpdateDag(new_dag)
            new_dag.changeFile(name, old_dag.name, old_py, old_json)
            t.parse_json(id)
            return redirect('/dagbuilderbaseview/')
        return self.render_template("/Dag/updateDag.html", old_dag=old_dag)

    @expose("/dag/<int:id>/delete", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def DeleteDag(self, id):
        d = Dag()
        
        lst_task = d.SelectAllTask(id)
        if not lst_task:
            d.DeleteDag(id)
            return redirect('/dagbuilderbaseview/')
        else:
            id = id
            return redirect(url_for('.ListTask', id=id))
        
        
    @expose("/task/<int:id>/view", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def ListTask(self, id):
        t = Task()
        d = Dag()
        lst_task = t.ListTask(id)
        return self.render_template("/Dag/task.html", lst_task= lst_task , dagid=id)

    @expose("/task/<int:id>/detail", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def DetailTask(self, id):
        t = Task()
        task = t.selecTask(id)
        id_dag = t.getId_Dag(id)
        return self.render_template("/Dag/detailTask.html", task=task, id_dag = id_dag)

    @expose("/task/<int:id>/insert", methods=['GET', 'POST'])
    # this method gets the view as localhost:/dagbuilderbaseview/
    @csrf.exempt  # if we don’t want to use csrf
    def insertTask(self, id):
        t = Task()
        d = Dag()
        All_Upstream = t.get_Upstream(id)
        priority_weight=None
        upstream=None
        pool=None
        filepath=None
        recursive=None
        min_size=None
        ignore_failed=None
        command_template=None
        interval=None
        dataset=None
        format_previous_date = None
        previous_date =None
        format_date= None
        flag = None
        number_of_days= None
        date= None
        if request.method == 'POST':
            name = request.form.get('name')
            priority_weight = request.form.get('priority_weight')
            pool = request.form.get('pool')
            upstream = request.form.getlist('nameUpstream')
            if name =="":
                return redirect(url_for('.ListTask', id=id))
            else:
                task_type = request.form.get('task_type')
                if task_type=='hdfs-sensor':
                    filepath = request.form.get('filepath')
                    flag = request.form.get('flag')
                    recursive = request.form.get('recursive')
                    min_size = request.form.get('min_size')
                    if request.form.getlist('option'):
                        number_of_days = request.form.get('number_of_days') 
                        interval = request.form.get('interval')

                        select = request.form.get('date')
                        forma_date = t.convertdate(int(select))
                        date = "{{ next_execution_date.strftime('{}') }}".format(forma_date)
                        date = '{'+date+'}'
                        format_date = t.format_date(int(select))
                elif task_type=="bash-sensor":
                    command_template = request.form.get('command_template')
                    if request.form.getlist('option'):
                        select = request.form.get('date')
                        forma_date = t.convertdate(int(select))
                        date = "{{ next_execution_date.strftime('{}') }}".format(forma_date)
                        date = '{'+date+'}'
                        format_date = t.format_date(int(select))
                elif task_type=="bash":
                    ignore_failed = request.form.get('ignore_failed')
                    command_template = request.form.get('command_template')
                    if request.form.getlist('option'):
                        select_date = request.form.get('date')
                        forma_date = t.convertdate(int(select_date))
                        date = "{{ next_execution_date.strftime('{}') }}".format(forma_date)
                        date = '{'+date+'}'
                        format_date = t.format_date(int(select_date))

                        select_previousDate = request.form.get('previousdate')
                        format_previous_date = t.convertdate(int(select_previousDate))
                        previous_date = "{{ next_execution_date.strftime('{}') }}".format(format_previous_date)
                        previous_date = '{'+previous_date+'}'
                        format_previous_date = t.format_date(int(select_previousDate))
                        dataset = request.form.get('dataset')

                task = Task(id, name, task_type, priority_weight, pool, upstream, filepath, flag, recursive, min_size, ignore_failed, command_template, number_of_days,
                            interval, date, dataset)
                
                task.insertTask(upstream, task, id, format_date, format_previous_date, previous_date)
                task.parse_json(id)
                d.bash_config(id)

                return redirect(url_for('.ListTask', id=id))

        return self.render_template("/Dag/insertTask.html", All_Upstream=All_Upstream, dag_id = id, task=t)


    @expose("/task/<int:id>/update", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def updateTask(self, id):
        t = Task()
        d = Dag()
        idDag = t.getId_Dag(id)
        All_Upstream = t.get_Upstream(idDag)
        task = t.selecTask(id)
        All_Upstream = t.remove_common(All_Upstream, task.upstreams, task.name)
        priority_weight=None
        upstream=None
        pool=None
        filepath=None
        recursive=None
        min_size=None
        ignore_failed=None
        command_template=None
        flag = None
        if request.method == 'POST' :
            name = request.form.get('name')
            task_type = request.form.get('task_type')
            priority_weight = request.form.get('priority_weight')
            upstream = request.form.getlist('nameUpstream')
            pool = request.form.get('pool')
            if task_type=='hdfs-sensor':
                filepath = request.form.get('filepath')
                flag = request.form.get('flag')
                recursive = request.form.get('recursive')
                min_size = request.form.get('min_size')
            elif task_type=='bash':
                ignore_failed = request.form.get('ignore_failed')
                command_template = request.form.get('command_template')
            else :
                command_template = request.form.get('command_template')
            
            task = Task(id, name, task_type, priority_weight, pool, upstream, filepath, flag, recursive, min_size, ignore_failed, command_template)
            task.updateTask(task)

            task.parse_json(idDag)
            d.bash_config(idDag)

            return redirect(url_for('.DetailTask', id=id))
        return self.render_template("/Dag/updateTask.html", task=task, All_Upstream=All_Upstream, list_up=task.upstreams)

    @expose("/task/<int:id>/delete", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def deleteTask(self, id):
        t = Task()
        d = Dag()
        id_dag = t.getId_Dag(id)
        t.deleteTask(id)

        t.parse_json(id_dag)
        d.bash_config(id_dag)

        return redirect(url_for('.ListTask', id=id_dag))

    @expose("/option/<int:id>/view", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def ListOption(self, id):
        op = Option()

        paramOp = op.get_Option(id)
        type = op.getType(paramOp.taskid)
        command = op.get_command(paramOp.id)

        return self.render_template("/Dag/option.html", type = type, command=command, paramOp=paramOp)

    @expose("/option/<int:id>/update", methods=['GET', 'POST'])
    @csrf.exempt  # if we don’t want to use csrf
    def updateOption(self, id):
        op = Option()
        t = Task()
        command = op.get_command(id)
        paramOp = op.get_OptionfromId(id)
        type = op.getType(paramOp.taskid)
        id_dag = t.getId_Dag(paramOp.taskid)
        number_of_days = None
        interval= None
        date = None
        format_date = None
        format_previous_date = None
        select_date = None
        select_previous_date= None
        dataset = None
        previous_date=None
        if request.method == 'POST' :
            if type == 'hdfs-sensor':
                number_of_days = request.form.get('number_of_days')
                interval = request.form.get('interval')

                select_date = request.form.get('date')
                format_date = t.format_date(int(select_date))
                forma_date = t.convertdate(int(select_date))
                date = "{{ next_execution_date.strftime('{}') }}".format(forma_date)
                date = '{'+date+'}'
            elif type == 'bash':                
                dataset = request.form.get('dataset')

                select_previous_date = request.form.get('previous_date')
                select_date = request.form.get('date')
                format_date = t.format_date(int(select_date))
                forma_date = t.convertdate(int(select_date))
                date = "{{ next_execution_date.strftime('{}') }}".format(forma_date)
                date = '{'+date+'}'

                format_previous_date = t.format_date(int(select_previous_date))
                forma_previous_date = t.convertdate(int(select_previous_date))
                previous_date = "{{ next_execution_date.strftime('{}') }}".format(forma_previous_date)
                previous_date = '{'+previous_date+'}'
            else:
                select_date = request.form.get('date')
                format_date = t.format_date(int(select_date))
                forma_date = t.convertdate(int(select_date))
                date = "{{ next_execution_date.strftime('{}') }}".format(forma_date)
                date = '{'+date+'}'

            op =Option(id,None,number_of_days, interval, date, format_date, format_previous_date)
            # if type =="bash" and type == "bash-sensor":
            #     op.updateCommand(dataset, date, previous_date, id)
            # elif type == "hdfs-sensor":
            op.updateOption(op)
            op.updateCommand(dataset, date, previous_date, id)
            t.parse_json(id_dag)
            
            return redirect(url_for('.ListOption', id = paramOp.taskid))
        return self.render_template("/Dag/updateOption.html", type = type, command=command, option=paramOp, task=t)


v_appbuilder_view = DAGBuilderBaseView()
# v_appbuilder_package_etl = ETLAppBuilderBaseView()
v_appbuilder_package = {
    "name": "Tao DAG",  # this is the name of the link displayed
    # This is the name of the tab under which we have our view
    "category": "Tao plugin",
    "view": v_appbuilder_view
}

class AirflowDAGPlugin(AirflowPlugin):
    name = "DAG_plugin"
    operators = []
    flask_blueprints = [bp]
    hooks = []
    executors = []
    admin_views = []
    # appbuilder_views = [v_appbuilder_package, v_appbuilder_package_etl]
    appbuilder_views = [v_appbuilder_package]