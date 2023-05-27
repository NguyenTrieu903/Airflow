from Connection import Connect

class Option():
    def __init__(self, id=None,taskid=None,number_of_days=None, interval=None, date=None,format_date=None,format_previous_date=None):
        self.id = id
        self.number_of_days = number_of_days
        self.interval = interval
        self.date = date
        self.format_date = format_date
        self.format_previous_date = format_previous_date
        self.taskid = taskid

    def getType(self, id):
        conn = Connect()

        select_task_type = """select task_type from task where id = %s"""
        value_id = (id,)
        type = conn.Select_item(select_task_type, value_id)
        return type

    def get_Option(self, id):
        conn = Connect()

        selectOption = """select id,task_id, number_of_days, interval, format_date, format_previous_date from option where task_id = %s"""
        value_id = (id,)
        data = conn.SelectAll_item(selectOption, value_id)
        # type = self.getType(id)
        for option in data:
            op = Option(option[0], option[1], option[2], option[3], option[4], option[5])
            return op

    def get_OptionfromId(self, id):
        conn = Connect()

        selectOption = """select task_id, number_of_days, interval, format_date, format_previous_date from option where id = %s"""
        value_id = (id,)
        data = conn.SelectAll_item(selectOption, value_id)
        # type = self.getType(id)
        for option in data:
            op = Option(None,option[0], option[1], option[2], None,option[3], option[4])
            return op

    def get_command(self, id):
        conn = Connect()

        selectCommand = """select dataset, date, previous_date from command where option_id = %s """
        value_id = (id,)
        data = conn.SelectAll_item(selectCommand, value_id)
        lst=[]
        for c in data:
            lst.append(c[0])
            lst.append(c[1])
            lst.append(c[2])
        return lst

    def updateOption(self, option):
        conn = Connect()

        updateOption = """update option set number_of_days = %s, interval=%s, date=%s, format_date=%s, format_previous_date= %s where id = %s"""
        value = (option.number_of_days, option.interval, option.date, option.format_date, option.format_previous_date, option.id)
        conn.Insert_item(updateOption, value)

    def updateCommand(self, dataset, date, previous_date,idOption):
        conn = Connect()

        updateOption = """update command set dataset = %s, date=%s, previous_date=%s where option_id = %s"""
        value = (dataset, date, previous_date,idOption)
        conn.Insert_item(updateOption, value)
    
    



