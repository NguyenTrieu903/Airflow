import psycopg2
import yaml
# from urlparse import urlparse # for python 3+ use: from urllib.parse import urlparse
from urllib.parse import urlparse
import os
class Connect():
    # def __init__(self):
    #     current_directory = os.getcwd()
    def connect(self):
        conn = None
        try:
            conn = psycopg2.connect(
                        user="postgres",
                        password="root",
                        host="localhost",
                        port="5432",
                        database="airflow"
                    )
            cursor = conn.cursor()
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into mobile table", error)
        finally:
            if conn is not None:
                return conn, cursor

    def Insert_item(self,query, paramater):
        conn, cursor = self.connect()
        if paramater is None:
            cursor.execute(query)
        else:
            cursor.execute(query,paramater)
        conn.commit()
        # count = cursor.rowcount
        # print(count, "Record inserted successfully into mobile table")
        cursor.close()
        conn.close()

    def SelectAll_item(self,query,para=None):
        conn, cursor = self.connect()
        if para is None:
            cursor.execute(query)
        else:
            cursor.execute(query,para)
        data = cursor.fetchall()
        cursor.close()
        return data

    def Select_item(self,query,para=None):
        conn, cursor = self.connect()
        if para is None:
            cursor.execute(query)
        else:
            cursor.execute(query,para)
        data = cursor.fetchall()
        res =""
        for row in data:
            res=row[0]
        cursor.close()
        return res
    
    def cc(self):
        current_directory = os.getcwd()
        arr = os.listdir(current_directory)
        return current_directory
    def get_Para(self):
        location_file = "{current_directory}/docker-compose.yaml".format(current_directory=self.cc())
        with open(location_file, 'r') as stream:
            try:
                code = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        Strconnect = code['x-airflow-common'].get('environment').get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
        result = urlparse(Strconnect)
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        port = result.port

        return username, password, database, hostname, port
    def connect1(self):
        # c = Connect()
        # location_file = "{current_directory}/docker-compose.yaml".format(current_directory=c.current_directory)
        # with open(location_file, 'r') as stream:
        #     try:
        #         code = yaml.load(stream)
        #     except yaml.YAMLError as exc:
        #         print(exc)
        # # result = urlparse("postgresql://postgres:postgres@localhost/postgres")
        # Strconnect = code['x-airflow-common'].get('environment').get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
        # result = urlparse(Strconnect)
        # username = result.username
        # password = result.password
        # database = result.path[1:]
        # hostname = result.hostname
        # port = result.port
        c = Connect()
        username, password, database, hostname, port = c.get_Para()
        # print(username, password, database, hostname, port)
        conn = None
        try:
            conn = psycopg2.connect(
                user=username,
                password=password,
                host=hostname,
                port=port,
                database=database
            )
            cursor = conn.cursor()
        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into mobile table", error)
        finally:
            if conn is not None:
                return conn, cursor

if __name__ == "__main__":
    c = Connect()
    c.cc()
    # c.connect1()
    # print(c.get_Para())
    